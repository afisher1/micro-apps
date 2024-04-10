import importlib
from argparse import ArgumentParser
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

import cimgraph.utils as cimUtils
from cimgraph.databases import ConnectionParameters
from cimgraph.databases.gridappsd.gridappsd import GridappsdConnection
from cimgraph.models import FeederModel
from gridappsd import GridAPPSD, topics
from gridappsd.simulation import *

CIM_PROFILE = 'rc4_2021'
IEC61970_301 = 7
cim = importlib.import_module(f'cimgraph.data_profile.{CIM_PROFILE}')
params = ConnectionParameters(cim_profile=CIM_PROFILE, iec61970_301=IEC61970_301)
DB_CONNECTION = GridappsdConnection(params)
GAD_OBJECT = GridAPPSD()
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
                    headers: A dictionary containing header information on the message recieved from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message recieved from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            enableControl(headers:Dict[Any], message:Dict[Any]): This callback function will tell the application that
                    it is allowed to perform cvr on the systems or simulations its tied to.
                Arguments:
                    headers: A dictionary containing header information on the message recieved from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message recieved from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            disableControl(headers:Dict[Any], message:Dict[Any]): This callback function will prevent the application
                    from performing cvr on the systems or simulations its tied to.
                Arguments:
                    headers: A dictionary containing header information on the message recieved from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message recieved from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            on_measurement(headers:Dict[Any], message:Dict[Any]): This callback function will be used to update the
                    applications measurements dictionary needed for cvr control.
                Arguments:
                    headers: A dictionary containing header information on the message recieved from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message recieved from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            cvr_control(): This is the main function for performing the cvr control.
    """
    period = 600

    def __init__(self,
                 gad_obj: GridAPPSD,
                 model_id: str,
                 period: int = None,
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
        self.measurements = {}
        if period is None:
            self.period = ConservationVoltageReductionController.period
        else:
            self.period = period
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
                        self.controllable_regulators[mRID][f'max_tap_{transformerEnd.phases.value}'] = \
                            transformerEnd.RatioTapChanger.highStep
                        self.controllable_regulators[mRID][f'min_tap_{transformerEnd.phases.value}'] = \
                            transformerEnd.RatioTapChanger.lowStep
        # Store measurements of voltages, loads, pv, battery, capacitor status, and regulator taps.
        measurements = self.graph_model.graph.get(cim.Analog, {})
        measurements.update(self.graph_model.graph.get(cim.Discrete, {}))
        for mrid, meas in measurements.items():
            if meas.measurementType == 'PNV':    #it's a voltage measurement. store it.
                self.measurements[mrid] = {'measurement_object': meas, 'measurement_value': {}}
            elif meas.measurementType == 'VA':    #it's a power measurement.
                if isinstance(meas.PowerSystemResource, (cim.EnergyConsumer, cim.PowerElectronicsConnection)):
                    self.measurements[mrid] = {'measurement_object': meas, 'measurement_value': {}}
            elif meas.measurementType == 'Pos':
                self.measurements[mrid] = {'measurement_object': meas, 'measurement_value': {}}
        if self.simulation is not None:
            self.simulation.start_simulation()

    def on_measurement(self, sim: Simulation, timestamp: str, measurements: Dict[str, Dict]):
        for mrid in self.measurements.keys():
            meas = measurements.get(mrid)
            if meas is not None:
                self.measurements[mrid]['measurement_value'] = meas
        #TODO: call cvr algorithm
        self.simulation.resume()
        #TODO: check for voltage violations and adjust cvr setpoints accordingly

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
            }
        }
        base_dss_str = self.gad_obj.get_response(topics.CONFIG, message)
        base_dss_str = base_dss_str.replace('{"data":', '')
        endOfBase = base_dss_str.find('calcv\n') + len('calcv\n')
        fileDir = Path(__file__).parent / 'app_instances' / f'{self.id}' / 'master.dss'
        with fileDir.open(mode='w') as f_master:
            f_master.write(base_dss_str[:endOfBase])


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
                              run_realtime=True,
                              pause_after_measurements=False)
    sim_config = SimulationConfig(power_system_config=psc, simulation_config=sim_args)
    sim_obj = Simulation(gapps=gad_obj, run_config=sim_config)
    return sim_obj


def main(control_enabled: bool, start_simulations: bool, model_id: str = None):
    if not isinstance(control_enabled, bool):
        raise TypeError(f'Argument, control_enabled, must be a boolean.')
    if not isinstance(start_simulations, bool):
        raise TypeError(f'Argument, start_simulations, must be a boolean.')
    if not isinstance(model_id, str) and model_id is not None:
        raise TypeError('The model id passed to the convervation voltage reduction application must be a string type '
                        f'or {None}.')
    platform_simulations = {}
    local_simulations = {}
    app_instances = {'field_instances': {}, 'external_simulation_instances': {}, 'local_simulation_instances': {}}
    response = GAD_OBJECT.query_model_info()
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
            local_simulations[m.get('modelId', '')] = createSimulation(GAD_OBJECT, m)
    else:
        #TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
        pass
    # Create an cvr controller instance for all the real systems in the database
    for m in models:
        m_id = m.get('modelId')
        app_instances['field_instances'][m_id] = ConservationVoltageReductionController(GAD_OBJECT, m_id)
    for sim_id, m_id in platform_simulations.items():
        app_instances['external_simulation_instances'][sim_id] = ConservationVoltageReductionController(GAD_OBJECT,
                                                                                                        m_id,
                                                                                                        sim_id=sim_id)
    for m_id, simulation in local_simulations.items():
        app_instances['local_simulation_instances'][m_id] = \
            ConservationVoltageReductionController(GAD_OBJECT, m_id, simulation=simulation)
    app_instances_exist = False
    if len(app_instances['field_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['external_simulation_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['local_simulation_instances']) > 0:
        app_instances_exist = True
    while app_instances_exist:
        try:
            app_instances_exist = False
            if len(app_instances['field_instances']) > 0:
                app_instances_exist = True
            elif len(app_instances['external_simulation_instances']) > 0:
                app_instances_exist = True
            elif len(app_instances['local_simulation_instances']) > 0:
                app_instances_exist = True
        except KeyboardInterrupt:
            break


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
