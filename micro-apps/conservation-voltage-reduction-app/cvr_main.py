import importlib
import math
import os
from argparse import ArgumentParser
from datetime import datetime
from typing import Any, Dict, Union
from uuid import uuid4

import cimgraph.utils as cimUtils
from cimgraph.databases import ConnectionParameters
from cimgraph.databases.gridappsd.gridappsd import GridappsdConnection
from cimgraph.models import FeederModel
from gridappsd import GridAPPSD, topics
from gridappsd.simulation import *
from gridappsd.utils import ProcessStatusEnum
from opendssdirect import dss

cim = None
DB_CONNECTION = None
CIM_GRAPH_MODELS = {}


class ConservationVoltageReductionController(object):
    """CVR Control Class
        This class implements a centralized algorithm for conservation voltage reduction by controlling regulators and
        capacitors in the distribution model.

        Attributes:
            sim_id: The simulation id that the instance will be perfoming cvr on. If the sim_id is None then the
                    application will assume it is performing cvr on the actual field devices.
                Type: str.
                Default: None.
            period: The amount of time to wait before calculating new cvr setpoints.
                Type: int.
                Default: None.
        Methods:
            initialize(headers:Dict[Any], message:Dict[Any]): This callback function will trigger the application to
                    read the model database to find all the measurements in the model as well as the controllable
                    regulators and capacitors in the sytem.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            enableControl(headers:Dict[Any], message:Dict[Any]): This callback function will tell the application that
                    it is allowed to perform cvr on the systems or simulations its tied to.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            disableControl(headers:Dict[Any], message:Dict[Any]): This callback function will prevent the application
                    from performing cvr on the systems or simulations its tied to.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            on_measurement(headers:Dict[Any], message:Dict[Any]): This callback function will be used to update the
                    applications measurements dictionary needed for cvr control.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            cvr_control(): This is the main function for performing the cvr control.
    """
    period = 600
    lower_voltage_limit_pu = 0.9

    def __init__(self,
                 gad_obj: GridAPPSD,
                 model_id: str,
                 period: int = None,
                 low_volt_lim: float = None,
                 sim_id: str = None,
                 simulation: Simulation = None):
        if not isinstance(gad_obj, GridAPPSD):
            raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
        if not isinstance(model_id, str):
            raise TypeError(f'model_id must be a str type.')
        if model_id is None or model_id == '':
            raise ValueError(f'model_id must be a valid uuid.')
        if not isinstance(period, int) and period is not None:
            raise TypeError(f'period must be an int type or {None}!')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'sim_id must be a string type or {None}!')
        if not isinstance(simulation, Simulation) and simulation is not None:
            raise TypeError(f'The simulation arg must be a Simulation type or {None}!')
        self.id = uuid4()
        self.platform_measurements = {}
        self.last_setpoints = {}
        self.desired_setpoints = {}
        self.controllable_regulators = {}
        self.controllable_capacitors = {}
        self.pnv_measurements = {}
        self.pnv_measurements_pu = {}
        self.va_measurements = {}
        self.pos_measurements = {}
        if period is None:
            self.period = ConservationVoltageReductionController.period
        else:
            self.period = period
        if low_volt_lim is None:
            self.low_volt_lim = ConservationVoltageReductionController.lower_voltage_limit_pu
        else:
            self.low_volt_lim = low_volt_lim
        self.measurements_topic = None
        self.simulation = None
        if simulation is not None:
            self.simulation = simulation
            self.simulation.add_onmeasurement_callback(self.on_measurement)
        if sim_id is None:
            measurements_topic = topics.field_output_topic(None, sim_id)
        else:
            measurements_topic = topics.simulation_output_topic(sim_id)
        self.gad_obj = gad_obj
        self.log = self.gad_obj.get_logger()
        self.gad_obj.subscribe(measurements_topic, self.on_measurement_callback)
        # Read model_id from cimgraph to get all the controllable regulators and capacitors, and measurements.
        self.graph_model = buildGraphModel(model_id)
        # Store controllable_capacitors
        self.controllable_capacitors = self.graph_model.graph.get(cim.LinearShuntCompensator, {})
        # Store controllable_regulators
        powerTransformers = self.graph_model.graph.get(cim.PowerTransformer, {})
        for mRID, powerTransformer in powerTransformers.items():
            for transformerTank in powerTransformer.TransformerTanks:
                for transformerEnd in transformerTank.TransformerTankEnds:
                    if transformerEnd.RatioTapChanger is not None and transformerEnd.Terminal.sequenceNumber == 1:
                        if mRID not in self.controllable_regulators.keys():
                            self.controllable_regulators[mRID] = {}
                        self.controllable_regulators[mRID]['name'] = f'reg_{powerTransformer.name}'
                        self.controllable_regulators[mRID][f'ratioTapChanger'] = transformerEnd.RatioTapChanger
        # Store measurements of voltages, loads, pv, battery, capacitor status, regulator taps, switch states.
        measurements = self.graph_model.graph.get(cim.Analog, {})
        measurements.update(self.graph_model.graph.get(cim.Discrete, {}))
        for meas in measurements.values():
            if meas.measurementType == 'PNV':    #it's a voltage measurement. store it.
                mrid = meas.mRID
                self.pnv_measurements[mrid] = {'measurement_object': meas, 'measurement_value': None}
            elif meas.measurementType == 'VA':    #it's a power measurement.
                if isinstance(meas.PowerSystemResource, (cim.EnergyConsumer, cim.PowerElectronicsConnection)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.va_measurements.keys():
                        self.va_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.va_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.va_measurements[mrid]['measurement_values'][meas.mRID] = None
            elif meas.measurementType == 'Pos':
                if isinstance(meas.PowerSystemResource, (cim.Switch, cim.PowerTransformer, cim.LinearShuntCompensator)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.pos_measurements.keys():
                        self.pos_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.pos_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.pos_measurements[mrid]['measurement_values'][meas.mRID] = None
        if self.simulation is not None:
            self.simulation.start_simulation()
        self.dssContext = dss.NewContext()
        self.create_opendss_context()

    def on_measurement(self, sim: Simulation, timestamp: str, measurements: Dict[str, Dict]):
        if not isinstance(sim, Simulation):
            self.log.error('')
        for mrid in self.pnv_measurements.keys():
            meas = measurements.get(mrid)
            if meas is not None:
                self.pnv_measurements[mrid]['measurement_value'] = meas
        for psr_mrid in self.va_measurements.keys():
            for mrid in self.va_measurements[psr_mrid]['measurement_values'].keys():
                meas = measurements.get(mrid)
                if meas is not None:
                    self.va_measurements[psr_mrid]['measurement_values'][mrid] = meas
        for psr_mrid in self.pos_measurements.keys():
            for mrid in self.pos_measurements[psr_mrid]['measurement_values'].keys():
                meas = measurements.get(mrid)
                if meas is not None:
                    self.pos_measurements[psr_mrid]['measurement_values'][mrid] = meas
        #TODO: call cvr algorithm
        if self.simulation is not None:
            # self.simulation.pause()
            self.simulation.resume()
        #TODO: check for voltage violations and adjust cvr setpoints accordingly

    def calculate_per_unit_voltage(self):
        for mrid in self.pnv_measurements.keys():
            self.pnv_measurements_pu[mrid] = None
            meas_base = None
            meas = self.pnv_measurements.get(mrid)
            if (meas is None) or (meas.get('measurement_value') is None):
                self.log.warn(f'Measurement not received yet for {mrid}. Retrying in {self.period}s.')
                return
            meas_value = meas.get('measurement_value').get('magnitude')
            if meas_value is None:
                self.log.error(f'The measurement value received from the platform for mrid {mrid} was corrupted.')
                return
            meas_obj = meas.get('measurement_object')
            if (meas_obj is None) or (not isinstance(meas_obj, cim.Measurement)):
                self.log.error(f'The measurement dictionary for mrid {mrid} is missing from the CIM database.')
                return
            meas_term = meas_obj.Terminal
            if isinstance(meas_term, cim.Terminal):
                if isinstance(meas_term.ConductingEquipment, cim.ConductingEquipment):
                    if isinstance(meas_term.ConductingEquipment.BaseVoltage, cim.BaseVoltage):
                        meas_base = meas_term.ConductingEquipment.BaseVoltage.nominalVoltage
            if (meas_base is None) or (meas_base < 1e-10):
                self.log.error(f'Unable to get the nominal voltage for measurement with mrid {mrid}.')
                return
            self.pnv_measurements_pu[mrid] = meas_value / meas_base

    def on_measurement_callback(self, header: Dict[str, Any], message: Dict[str, Any]):
        timestamp = message.get('message', {}).get('timestamp', '')
        measurements = message.get('message', {}).get('measurements', {})
        self.on_measurement(None, timestamp, measurements)

    def create_opendss_context(self):
        message = {
            'configurationType': 'DSS Base',
            'parameters': {
                'i_fraction': '1.0',
                'z_fraction': '0.0',
                'model_id': self.graph_model.container.mRID,
                'load_scaling_factor': '1.0',
                'schedule_name': 'ieeezipload',
                'p_fraction': '0.0'
            },
            'resultFormat': 'JSON'
        }
        base_dss_response = self.gad_obj.get_response(topics.CONFIG, message)
        base_dss_dict = json.loads(base_dss_response.get('message', ''), strict=False)
        base_dss_str = base_dss_dict.get('data', '')
        fileDir = Path(__file__).parent / 'cvr_app_instances' / f'{self.id}' / 'master.dss'
        fileDir.parent.mkdir(parents=True, exist_ok=True)
        endOfBase = base_dss_str.find('calcv\n') + len('calcv\n')
        with fileDir.open(mode='w') as f_master:
            f_master.write(base_dss_str[:endOfBase])
        self.dssContext.Command(f'Redirect {fileDir}')
        self.dssContext.Command(f'Compile {fileDir}')
        self.dssContext.Solution.SolveNoControl()

    def update_opendss_with_measurements(self):
        for psr_mrid in self.va_measurements.keys():
            va_val = complex(0.0, 0.0)
            val_is_valid = True
            for meas_mrid in self.va_measurements[psr_mrid]['measurement_values'].keys():
                meas_val = self.va_measurements[psr_mrid]['measurement_values'][meas_mrid]
                if meas_val is not None:
                    va_val += complex(
                        meas_val.get('magnitude') * math.cos(math.radians(meas_val.get('angle'))),
                        meas_val.get('magnitude') * math.sin(math.radians(meas_val.get('angle')))) / 1000.0
                else:
                    val_is_valid = False
                    break
            if val_is_valid:
                meas_obj = self.va_measurements[psr_mrid]['measurement_objects'][meas_mrid].PowerSystemResource
                name = meas_obj.name
                if isinstance(meas_obj, cim.EnergyConsumer):
                    self.dssContext.Command(f'Load.{name}.kw={va_val.real}')
                    self.dssContext.Command(f'Load.{name}.kvar={va_val.imag}')
                elif isinstance(meas_obj, cim.PowerElectronicsConnection):
                    if isinstance(meas_obj.PowerElectronicsUnit, cim.PhotovoltaicUnit):
                        self.dssContext.Command(f'PVSystem.{name}.pmpp={va_val.real}')
                        self.dssContext.Command(f'PVSystem.{name}.kvar={va_val.imag}')
                    elif isinstance(meas_obj.PowerElectronicsUnit, cim.BatteryUnit):
                        self.dssContext.Command(f'Storage.{name}.kw={va_val.real}')
                        self.dssContext.Command(f'Storage.{name}.kvar={va_val.imag}')
                    #TODO: update wind turbines in opendss
                #TODO: update other generator/motor type object in opendss
        for psr_mrid in self.pos_measurements.keys():
            num_of_meas = len(self.pos_measurements[psr_mrid]['measurement_objects'].keys())
            pos_val = [0] * num_of_meas
            val_is_valid = True
            key_list = list(self.pos_measurements[psr_mrid]['measurement_values'].keys())
            for meas_mrid in key_list:
                meas_val = self.pos_measurements[psr_mrid]['measurement_values'][meas_mrid]
                if meas_val is not None:
                    pos_val[key_list.index(meas_mrid)] = meas_val.get('value')
                else:
                    val_is_valid = False
                    break
            if val_is_valid:
                meas_obj = self.pos_measurements[psr_mrid]['measurement_objects'][meas_mrid].PowerSystemResource
                name = meas_obj.name
                if isinstance(meas_obj, cim.Switch):
                    switch_state = sum(pos_val)
                    if switch_state == 0:
                        self.dssContext.Command(f'close Line.{name} 1')
                    else:
                        self.dssContext.Command(f'open Line.{name} 1')
                elif isinstance(meas_obj, cim.LinearShuntCompensator):
                    self.dssContext.Command(f'Capacitor.{name}.states={pos_val}')
                if isinstance(meas_obj, cim.PowerTransformer):
                    reg = self.dssContext.Transformers.First()
                    while reg:
                        if self.dssContext.Transformers.Name() == name:
                            break
                        else:
                            reg = self.dssContext.Transformers.Next()
                    regulationPerStep = (self.dssContext.Transformers.MaxTap() - self.dssContext.Transformers.MinTap()
                                         ) / self.dssContext.Transformers.NumTaps()
                    tapStep = 1.0 + (pos_val[0] * regulationPerStep)
                    self.dssContext.Command(f'Transformer.{name}.Taps=[1.0, {tapStep}]')

    def cvr_control(self):
        capacitor_list = []
        for cap_mrid, cap in self.controllable_capacitors.items():
            cap_meas_dict = self.pos_measurements.get(cap_mrid)
            pos = None
            if cap_meas_dict is not None:
                cap_meas_val_dict = cap_meas_dict.get('measurement_values')
                pos_list = list(cap_meas_val_dict.values())
                if pos_list:
                    pos = pos_list[0]
            if pos is not None:
                # For a capacitor, pos = 1 means the capacitor is connected to the grid. Otherwise, it's 0.
                capacitor_list.append((cap_mrid, cap, cap.bPerSection, pos, True))
        if capacitor_list:
            meas_list = []
            # FIXMEOr: Is this going to pnv_measurements_pu too many times?
            for meas_mrid in self.pnv_measurements_pu.keys():
                if self.pnv_measurements_pu[meas_mrid] is not None:
                    meas_list.append(self.pnv_measurements_pu[meas_mrid])
            if meas_list:
                if min(meas_list) > self.low_volt_lim:
                    sorted(capacitor_list, key=lambda x: x[2], reverse=True)
                    local_capacitor_list = []
                    for element_tuple in capacitor_list:
                        if element_tuple[2] == 1:
                            local_capacitor_list.append(element_tuple)
                    commands_dict = self.decrease_voltage_capacitor(local_capacitor_list)
                else:
                    sorted(capacitor_list, key=lambda x: x[2])
                    local_capacitor_list = []
                    for element_tuple in capacitor_list:
                        if element_tuple[2] == 0:
                            local_capacitor_list.append(element_tuple)
                    commands_dict = self.increase_voltage_capacitor(local_capacitor_list)

        for mrid in commands_dict.keys():
            self.send_setpoint(commands_dict[mrid]['object'], commands_dict[mrid]['setpoint'])

    def increase_voltage_capacitor(self, cap_list: list) -> dict:
        return_dict = {}
        success = False
        while cap_list and not success:
            element_tuple = cap_list.pop(0)
            if not element_tuple[3]:
                continue
            cap_mrid = element_tuple[0]
            cap_obj = element_tuple[1]
            pos_val = [1] * len(self.pos_measurements[cap_mrid]['measurement_values'].keys())
            saved_states = self.dssContext.Command(f'Capacitor.{cap_obj.name}.states')
            self.dssContext.Command(f'Capacitor.{cap_obj.name}.states={pos_val}')
            self.dssContext.Solution.SolveNoControl()
            converged = self.dssContext.Solution.Converged()
            if converged:
                return_dict[cap_mrid] = {'setpoint': 1, 'object': cap_obj}
                if min(self.dssContext.Circuit.AllBusMagPu()) > self.low_volt_lim:
                    success = True
            else:
                self.dssContext.Command(f'Capacitor.{cap_obj.name}.states={saved_states}')
        return return_dict

    def decrease_voltage_capacitor(self, cap_list: list) -> dict:
        return_dict = {}
        while cap_list:
            element_tuple = cap_list.pop(0)
            if not element_tuple[3]:
                continue
            cap_mrid = element_tuple[0]
            cap_obj = element_tuple[1]
            pos_val = [0] * len(self.pos_measurements[cap_mrid]['measurement_values'].keys())
            saved_states = self.dssContext.Command(f'Capacitor.{cap_obj.name}.states')
            self.dssContext.Command(f'Capacitor.{cap_obj.name}.states={pos_val}')
            self.dssContext.Solution.SolveNoControl()
            converged = self.dssContext.Solution.Converged()
            success = False
            if converged:
                if min(self.dssContext.Circuit.AllBusMagPu()) > self.low_volt_lim:
                    return_dict[cap_mrid] = {'setpoint': 0, 'object': cap_obj}
                    success = True
            if not success:
                self.dssContext.Command(f'Capacitor.{cap_obj.name}.states={saved_states}')
        return return_dict

    def send_setpoint(self, cimObject: object, setpoint: int):
        if not isinstance(cimObject, (cim.TapChanger, cim.LinearShuntCompensator)):
            raise TypeError('cimObject must be an instance of cim.TapChanger or cim.LinearShuntCompensator. cimObject '
                            f'provided was of type {type(cimObject)}.')
        if not isinstance(setpoint, int):
            raise TypeError(f'setpoint is expected to be an int. Provided type was {type(setpoint)}')
        if isinstance(cimObject, cim.TapChanger):
            pass


def buildGraphModel(mrid: str) -> FeederModel:
    if not isinstance(mrid, str):
        raise TypeError(f'The mrid passed to the convervation voltage reduction application must be a string.')
    if mrid not in CIM_GRAPH_MODELS.keys():
        feeder_container = cim.Feeder(mRID=mrid)
        graph_model = FeederModel(connection=DB_CONNECTION, container=feeder_container, distributed=False)
        graph_model.get_all_edges(cim.PowerTransformer)
        graph_model.get_all_edges(cim.TransformerTank)
        graph_model.get_all_edges(cim.Asset)
        graph_model.get_all_edges(cim.LinearShuntCompensator)
        graph_model.get_all_edges(cim.PowerTransformerEnd)
        graph_model.get_all_edges(cim.TransformerEnd)
        graph_model.get_all_edges(cim.TransformerMeshImpedance)
        graph_model.get_all_edges(cim.TransformerTankEnd)
        graph_model.get_all_edges(cim.TransformerTankInfo)
        graph_model.get_all_edges(cim.LinearShuntCompensatorPhase)
        graph_model.get_all_edges(cim.Terminal)
        graph_model.get_all_edges(cim.ConnectivityNode)
        graph_model.get_all_edges(cim.BaseVoltage)
        graph_model.get_all_edges(cim.TransformerEndInfo)
        graph_model.get_all_edges(cim.Analog)
        graph_model.get_all_edges(cim.Discrete)
        CIM_GRAPH_MODELS[mrid] = graph_model
    return CIM_GRAPH_MODELS[mrid]


def createSimulation(gad_obj: GridAPPSD, model_info: Dict[str, Any]) -> Simulation:
    if not isinstance(gad_obj, GridAPPSD):
        raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
    if not isinstance(model_info, dict):
        raise TypeError(f'The model_id must be a dictionary type.')
    line_name = model_info.get('modelId')
    subregion_name = model_info.get('subRegionId')
    region_name = model_info.get('regionId')
    sim_name = model_info.get('modelName')
    if line_name is None:
        raise ValueError(f'Bad model info dictionary. The dictionary is missing key modelId.')
    if subregion_name is None:
        raise ValueError(f'Bad model info dictionary. The dictionary is missing key subRegionId.')
    if region_name is None:
        raise ValueError(f'Bad model info dictionary. The dictionary is missing key regionId.')
    if sim_name is None:
        raise ValueError(f'Bad model info dictionary. The dictionary is missing key modelName.')
    psc = PowerSystemConfig(Line_name=line_name,
                            SubGeographicalRegion_name=subregion_name,
                            GeographicalRegion_name=region_name)
    start_time = int(datetime.utcnow().replace(microsecond=0).timestamp())
    sim_args = SimulationArgs(start_time=f'{start_time}',
                              duration=f'{24*3600}',
                              simulation_name=sim_name,
                              run_realtime=False,
                              pause_after_measurements=True)
    sim_config = SimulationConfig(power_system_config=psc, simulation_config=sim_args)
    sim_obj = Simulation(gapps=gad_obj, run_config=sim_config)
    return sim_obj


def createGadObject() -> GridAPPSD:
    gad_user = os.environ.get('GRIDAPPSD_USER')
    if gad_user is None:
        os.putenv('GRIDAPPSD_USER', 'system')
    gad_password = os.environ.get('GRIDAPPSD_PASSWORD')
    if gad_password is None:
        os.putenv('GRIDAPPSD_PASSWORD', 'manager')
    gad_app_id = os.environ.get('GRIDAPPSD_APPLICATION_ID')
    if gad_password is None:
        os.putenv('GRIDAPPSD_APPLICATION_ID', 'ConservationVoltageReductionApplication')
    return GridAPPSD()


def main(control_enabled: bool, start_simulations: bool, model_id: str = None):
    if not isinstance(control_enabled, bool):
        raise TypeError(f'Argument, control_enabled, must be a boolean.')
    if not isinstance(start_simulations, bool):
        raise TypeError(f'Argument, start_simulations, must be a boolean.')
    if not isinstance(model_id, str) and model_id is not None:
        raise TypeError(
            f'The model id passed to the convervation voltage reduction application must be a string type or {None}.')
    global cim, DB_CONNECTION
    cim_profile = 'rc4_2021'
    iec61970_301 = 7
    cim = importlib.import_module(f'cimgraph.data_profile.{cim_profile}')
    params = ConnectionParameters(cim_profile=cim_profile, iec61970_301=iec61970_301)
    DB_CONNECTION = GridappsdConnection(params)
    gad_object = createGadObject()
    gad_log = gad_object.get_logger()
    platform_simulations = {}
    local_simulations = {}
    app_instances = {'field_instances': {}, 'external_simulation_instances': {}, 'local_simulation_instances': {}}
    response = gad_object.query_model_info()
    models = response.get('data', {}).get('models', [])
    model_is_valid = False
    if model_id is not None:
        for m in models:
            m_id = m.get('modelId')
            if model_id == m_id:
                model_is_valid = True
                break
        if not model_is_valid:
            raise ValueError(f'The model id provided does not exist in the GridAPPS-D plaform.')
        else:
            models = [m]
    if start_simulations:
        for m in models:
            local_simulations[m.get('modelId', '')] = createSimulation(gad_object, m)
    else:
        #TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
        pass
    # Create an cvr controller instance for all the real systems in the database
    # for m in models:
    #     m_id = m.get('modelId')
    #     app_instances['field_instances'][m_id] = ConservationVoltageReductionController(gad_object, m_id)
    for sim_id, m_id in platform_simulations.items():
        app_instances['external_simulation_instances'][sim_id] = ConservationVoltageReductionController(gad_object,
                                                                                                        m_id,
                                                                                                        sim_id=sim_id)
    for m_id, simulation in local_simulations.items():
        app_instances['local_simulation_instances'][m_id] = ConservationVoltageReductionController(
            gad_object, m_id, simulation=simulation)
    app_instances_exist = False
    if len(app_instances['field_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['external_simulation_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['local_simulation_instances']) > 0:
        app_instances_exist = True
    application_uptime = int(time.time())
    if app_instances_exist:
        gad_log.info('ConservationVoltageReductionApplication successfully started.')
        gad_object.set_application_status(ProcessStatusEnum.RUNNING)

    while gad_object.connected():
        try:
            app_instances_exist = False
            if len(app_instances['field_instances']) > 0:
                app_instances_exist = True
                application_uptime = int(time.time())
            if len(app_instances['external_simulation_instances']) > 0:
                app_instances_exist = True
                application_uptime = int(time.time())
            else:
                #TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
                pass
            if len(app_instances['local_simulation_instances']) > 0:
                app_instances_exist = True
                application_uptime = int(time.time())
            if not app_instances_exist:
                application_downtime = int(time.time()) - application_uptime
                if application_downtime > 3600:
                    gad_log.info('There have been no running instances ConservationVoltageReductionController '
                                 'instances for an hour. Shutting down the application.')
                    gad_object.set_application_status(ProcessStatusEnum.STOPPING)
                    gad_object.disconnect()
        except KeyboardInterrupt:
            gad_log.info('Manually exiting ConservationVoltageReductionApplication')
            gad_object.set_application_status(ProcessStatusEnum.STOPPING)
            gad_object.disconnect()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('model_id', nargs='?', default=None, help='The mrid of the cim model to perform cvr on.')
    parser.add_argument('-s',
                        '--start_simulations',
                        action='store_true',
                        help='Flag to have application start simulations')
    parser.add_argument('-d',
                        '--disable_control',
                        action='store_true',
                        help='Flag to disable control on startup by default.')
    args = parser.parse_args()
    main(args.disable_control, args.start_simulations, args.model_id)
