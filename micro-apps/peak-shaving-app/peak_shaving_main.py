import importlib
import math
import os
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Dict, Union
from uuid import uuid4

import cimgraph.utils as cimUtils
from cimgraph.databases import ConnectionParameters
from cimgraph.databases.blazegraph.blazegraph import BlazegraphConnection
from cimgraph.databases.gridappsd.gridappsd import GridappsdConnection
from cimgraph.models import FeederModel
from gridappsd import DifferenceBuilder, GridAPPSD, topics
from gridappsd.simulation import *
from gridappsd.utils import ProcessStatusEnum

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
            peak_shaving_control(): This is the main function for performing the peak shaving control.
    """

    def __init__(self,
                 gad_obj: GridAPPSD,
                 model_id: str,
                 peak_setpoint: float = None,
                 sim_id: str = None,
                 simulation: Simulation = None):
        if not isinstance(gad_obj, GridAPPSD):
            raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
        if not isinstance(model_id, str):
            raise TypeError(f'model_id must be a str type.')
        if model_id is None or model_id == '':
            raise ValueError(f'model_id must be a valid uuid.')
        if not isinstance(peak_setpoint, float) and peak_setpoint is not None:
            raise TypeError(f'peak_setpoint must be an float type or {None}!')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'sim_id must be a string type or {None}!')
        if not isinstance(simulation, Simulation) and simulation is not None:
            raise TypeError(f'The simulation arg must be a Simulation type or {None}!')
        self.id = uuid4()
        self.desired_setpoints = {}
        self.controllable_batteries_A = {}
        self.controllable_batteries_B = {}
        self.controllable_batteries_C = {}
        self.battery_va_measurements_A = {}
        self.battery_va_measurements_B = {}
        self.battery_va_measurements_C = {}
        self.battery_soc_measurements_A = {}
        self.battery_soc_measurements_B = {}
        self.battery_soc_measurements_C = {}
        self.peak_va_measurements_A = {}
        self.peak_va_measurements_B = {}
        self.peak_va_measurements_C = {}
        self.measurements_topic = None
        self.setpoints_topic = None
        self.simulation = None
        self.simulation_id = None    #TODO: figure out what simulation_id should be when deployed in the field.
        if simulation is not None:
            self.simulation = simulation
            self.simulation_id = simulation.simulation_id
            self.simulation.add_onmeasurement_callback(self.on_measurement)
        if sim_id is None:
            self.measurements_topic = topics.field_output_topic(None, sim_id)
        else:
            self.simulation_id = sim_id
            self.measurements_topic = topics.simulation_output_topic(sim_id)
        self.differenceBuilder = DifferenceBuilder(self.simulation_id)
        self.gad_obj = gad_obj
        self.log = self.gad_obj.get_logger()
        if self.measurements_topic:
            self.gad_obj.subscribe(self.measurements_topic, self.on_measurement_callback)
        if self.simulation_id:
            self.setpoints_topic = topics.simulation_input_topic(self.simulation_id)
        else:
            self.setpoints_topic = topics.field_input_topic()
        # Read model_id from cimgraph to get all the controllable regulators and capacitors, and measurements.
        self.graph_model = buildGraphModel(model_id)
        # Store controllable_capacitors
        # print(self.controllable_capacitors.keys())
        # Store controllable_regulators
        self.installed_battery_capacity_A = 0.0
        self.installed_battery_capacity_B = 0.0
        self.installed_battery_capacity_C = 0.0
        self.installed_battery_power_A = 0.0
        self.installed_battery_power_B = 0.0
        self.installed_battery_power_C = 0.0
        powerElectronicsConnection = self.graph_model.graph.get(cim.PowerElectronicsConnection, {})
        # print(powerTransformers.keys())
        # print(ratioTapChangers.keys())
        for mRID, powerElectronicsConnection in powerElectronicsConnection.items():
            hasBatterySource = False
            phase = self.find_primary_phase(powerElectronicsConnection)
            for powerElectronicsUnit in powerElectronicsConnection.PowerElectronicsUnit:
                if isinstance(powerElectronicsUnit, cim.BatteryUnit):
                    hasBatterySource = True
                    self.controllable_batteries[mRID] = powerElectronicsConnection
                    self.installed_battery_capacity += float(powerElectronicsUnit.ratedE)
            if hasBatterySource:
                self.installed_battery_power += float(powerElectronicsConnection.ratedS)
        # Store measurements of voltages, loads, pv, battery, capacitor status, regulator taps, switch states.
        # print(self.controllable_regulators.keys())
        measurements = self.graph_model.graph.get(cim.Analog, {})
        # print(measurements.keys())
        for meas in measurements.values():
            if meas.measurementType == 'VA':    # it's a power measurement.
                if isinstance(meas.PowerSystemResource, (cim.PowerElectronicsConnection)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.va_measurements.keys():
                        self.va_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.va_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.va_measurements[mrid]['measurement_values'][meas.mRID] = None
            elif meas.measurementType == 'SoC':    # it's a State of Charge measurement.
                if isinstance(meas.PowerSystemResource, (cim.Switch, cim.PowerTransformer, cim.LinearShuntCompensator)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.pos_measurements.keys():
                        self.pos_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.pos_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.pos_measurements[mrid]['measurement_values'][meas.mRID] = None
        # for r in self.controllable_regulators.values():
        #     print(json.dumps(r['PhasesToName'], indent=4, sort_keys=True))
        if self.simulation is not None:
            self.simulation.start_simulation()
        self.next_control_time = 0
        self.voltage_violation_time = -1
        self.isValid = True
        self.first_message = True

    def on_measurement(self, sim: Simulation, timestamp: str, measurements: Dict[str, Dict]):
        self.desired_setpoints.clear()
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
        self.calculate_per_unit_voltage()
        self.update_opendss_with_measurements()
        self.peak_shaving_control()
        if self.simulation is not None:
            self.simulation.resume()

    def on_measurement_callback(self, header: Dict[str, Any], message: Dict[str, Any]):
        timestamp = message.get('message', {}).get('timestamp', '')
        measurements = message.get('message', {}).get('measurements', {})
        self.on_measurement(None, timestamp, measurements)

    def peak_shaving_control(self):
        self.desired_setpoints = {}
        if self.desired_setpoints:
            self.send_setpoints()

    def send_setpoints(self):
        self.differenceBuilder.clear()
        for mrid, dictVal in self.desired_setpoints.items():
            cimObj = dictVal.get('object')
            newSetpoint = dictVal.get('setpoint')
            oldSetpoint = dictVal.get('old_setpoint')
            if isinstance(cimObj, cim.LinearShuntCompensator):
                self.differenceBuilder.add_difference(mrid, 'ShuntCompensator.sections', newSetpoint[0],
                                                      int(not newSetpoint[0]))
            elif isinstance(cimObj, cim.PowerTransformer):
                tapChangersList = self.controllable_regulators.get(mrid, {}).get('RatioTapChangers', [])
                currentTapPositions = self.pos_measurements.get(mrid, {})
                for rtp in tapChangersList:
                    phases = rtp.TransformerEnd.phases
                    for measurement_object in currentTapPositions.get('measurement_objects', {}).values():
                        if measurement_object.phases == phases:
                            currentSetpoint = currentTapPositions.get('measurement_values',
                                                                      {}).get(measurement_object.mRID, {}).get('value')
                            if not currentSetpoint:
                                self.differenceBuilder.add_difference(rtp.mRID, 'TapChanger.step', newSetpoint, 'NA')
                            else:
                                self.differenceBuilder.add_difference(rtp.mRID, 'TapChanger.step', newSetpoint,
                                                                      currentSetpoint)
                            break
            else:
                self.log.warning(f'The CIM object with mRID, {mrid}, is not a cim.LinearShuntCompensator or a '
                                 f'cim.PowerTransformer. The object is a {type(cimObj)}. This application will ignore '
                                 'sending a setpoint to this object.')
        setpointMessage = self.differenceBuilder.get_message()
        self.gad_obj.send(self.setpoints_topic, setpointMessage)

    def simulation_completed(self, sim: Simulation):
        self.log.info(f'Simulation for ConservationVoltageReductionController:{self.id} has finished. This application '
                      'instance can be deleted.')
        self.isValid = False

    def __del__(self):
        directoryToDelete = Path(__file__).parent / 'cvr_app_instances' / f'{self.id}'
        removeDirectory(directoryToDelete)


def buildGraphModel(mrid: str) -> FeederModel:
    global CIM_GRAPH_MODELS
    if not isinstance(mrid, str):
        raise TypeError(f'The mrid passed to the convervation voltage reduction application must be a string.')
    if mrid not in CIM_GRAPH_MODELS.keys():
        feeder_container = cim.Feeder(mRID=mrid)
        graph_model = FeederModel(connection=DB_CONNECTION, container=feeder_container, distributed=False)
        # graph_model.get_all_edges(cim.PowerTransformer)
        # graph_model.get_all_edges(cim.TransformerTank)
        # graph_model.get_all_edges(cim.Asset)
        # graph_model.get_all_edges(cim.LinearShuntCompensator)
        # graph_model.get_all_edges(cim.PowerTransformerEnd)
        # graph_model.get_all_edges(cim.TransformerEnd)
        # graph_model.get_all_edges(cim.TransformerMeshImpedance)
        # graph_model.get_all_edges(cim.TransformerTankEnd)
        # graph_model.get_all_edges(cim.TransformerTankInfo)
        # graph_model.get_all_edges(cim.RatioTapChanger)
        # graph_model.get_all_edges(cim.TapChanger)
        # graph_model.get_all_edges(cim.TapChangerControl)
        # graph_model.get_all_edges(cim.TapChangerInfo)
        # graph_model.get_all_edges(cim.LinearShuntCompensatorPhase)
        # graph_model.get_all_edges(cim.Terminal)
        # graph_model.get_all_edges(cim.ConnectivityNode)
        # graph_model.get_all_edges(cim.BaseVoltage)
        # graph_model.get_all_edges(cim.EnergySource)
        # graph_model.get_all_edges(cim.EnergyConsumer)
        # graph_model.get_all_edges(cim.ConformLoad)
        # graph_model.get_all_edges(cim.NonConformLoad)
        # graph_model.get_all_edges(cim.EnergyConsumerPhase)
        # graph_model.get_all_edges(cim.LoadResponseCharacteristic)
        # graph_model.get_all_edges(cim.PowerCutZone)
        # graph_model.get_all_edges(cim.Analog)
        # graph_model.get_all_edges(cim.Discrete)
        cimUtils.get_all_data(graph_model)
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
                              pause_after_measurements=False)
    sim_config = SimulationConfig(power_system_config=psc, simulation_config=sim_args)
    sim_obj = Simulation(gapps=gad_obj, run_config=sim_config)
    return sim_obj


def createGadObject() -> GridAPPSD:
    gad_user = os.environ.get('GRIDAPPSD_USER')
    if gad_user is None:
        os.environ['GRIDAPPSD_USER'] = 'system'
    gad_password = os.environ.get('GRIDAPPSD_PASSWORD')
    if gad_password is None:
        os.environ['GRIDAPPSD_PASSWORD'] = 'manager'
    gad_app_id = os.environ.get('GRIDAPPSD_APPLICATION_ID')
    if gad_app_id is None:
        os.environ['GRIDAPPSD_APPLICATION_ID'] = 'ConservationVoltageReductionApplication'
    return GridAPPSD()


def removeDirectory(directory: Path | str):
    if isinstance(directory, str):
        directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            removeDirectory(item)
        else:
            item.unlink()
    directory.rmdir()


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
    # params = ConnectionParameters(cim_profile=cim_profile, iec61970_301=iec61970_301)
    # DB_CONNECTION = GridappsdConnection(params)
    params = ConnectionParameters(url='http://localhost:8889/bigdata/namespace/kb/sparql',
                                  cim_profile=cim_profile,
                                  iec61970_301=iec61970_301)
    DB_CONNECTION = BlazegraphConnection(params)
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

    while gad_object.connected:
        try:
            app_instances_exist = False
            #TODO: check platform for any running external simulations to control.
            invalidInstances = []
            for m_id, app in app_instances.get('field_instances', {}).items():
                if not app.isValid:
                    invalidInstances.append(m_id)
            for m_id in invalidInstances:
                del app_instances['field_instances'][m_id]
            invalidInstances = []
            for sim_id, app in app_instances.get('external_simulation_instances', {}).items():
                #TODO: check if external simulation has finished and shutdown the corresponding app instance.
                if not app.isValid:
                    invalidInstances.append(sim_id)
            for sim_id in invalidInstances:
                del app_instances['external_simulation_instances'][sim_id]
            invalidInstances = []
            for m_id, app in app_instances.get('local_simulation_instances', {}).items():
                if not app.isValid:
                    invalidInstances.append(m_id)
            for m_id in invalidInstances:
                del app_instances['local_simulation_instances'][m_id]
            if len(app_instances['field_instances']) > 0:
                app_instances_exist = True
                application_uptime = int(time.time())
            if len(app_instances['external_simulation_instances']) > 0:
                app_instances_exist = True
                application_uptime = int(time.time())
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
            appList = list(app_instances.get('field_instances', {}))
            for app_mrid in appList:
                del app_instances['field_instances'][app_mrid]
            appList = list(app_instances.get('external_simulation_instances', {}))
            for app_mrid in appList:
                del app_instances['external_simulation_instances'][app_mrid]
            appList = list(app_instances.get('local_simulation_instances', {}))
            for app_mrid in appList:
                del app_instances['local_simulation_instances'][app_mrid]
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
