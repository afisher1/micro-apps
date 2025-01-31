import importlib
import logging
import math
import os
import csv
from cmath import exp
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import uuid4

import cimgraph.utils as cimUtils
from cimgraph.databases import ConnectionParameters
from cimgraph.databases.blazegraph.blazegraph import BlazegraphConnection
from cimgraph.databases.gridappsd.gridappsd import GridappsdConnection
from cimgraph.models import FeederModel
from gridappsd import DifferenceBuilder, GridAPPSD, topics
from gridappsd.simulation import *
from gridappsd.utils import ProcessStatusEnum

logging.basicConfig(format='%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s',
                    filename='peak_shaving.log',
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')
logger = logging.getLogger(__name__)
cim = None
DB_CONNECTION = None
CIM_GRAPH_MODELS = {}


IEEE123_APPS = "_F49D1288-9EC6-47DB-8769-57E2B6EDB124"
ieee123_apps_feeder_head_measurement_mrids = [
    "_903a0e85-6a11-4b8e-82cf-163376df7764",
    "_6d82c9d9-5f99-4356-8a8d-acda9cd6f17b",
    "_971765b5-da69-4ea8-a0cb-d2b9613b8ee3"
]

def add_data_to_csv(file_path, data, header=None):
    # Check if the file exists
    file_exists = os.path.isfile(file_path)

    # Open the CSV file in append mode (or create it if it doesn't exist)
    with open(file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        # If the file doesn't exist, write the header first
        if not file_exists and header:
            writer.writerow(header)
        # Write the data (a list or tuple representing a row)
        writer.writerow(data)


def findMeasurement(measurementsList, type: str, phases):
    rv = None
    for measurement in measurementsList:
        if measurement.measurementType == type and measurement.phases in phases:
            rv = measurement
            break
    if rv is None:
        raise RuntimeError(f'findMeasurement(): No measurement of type {type} exists for any of the given phases!.')
    return rv


def findMeasurements(measurementsList, type: str):
    rv = []
    for measurement in measurementsList:
        if measurement.measurementType == type:
            rv.append({'object': measurement, 'value': None})
    if not rv:
        raise RuntimeError(f'findMeasurements(): No measurement of type {type} exists!.')
    return rv


def findPrimaryPhase(cimObj):
    '''
        Helper function for finding the primary phase an instance of cim.ConductingEquipment on the secondary
        system is connected to.
    '''
    if not isinstance(cimObj, cim.ConductingEquipment):
        raise TypeError('findPrimaryPhase(): cimObj must be an instance of cim.ConductingEquipment!')
    equipmentToCheck = [cimObj]
    phaseCode = None
    xfmr = None
    i = 0
    while not xfmr and i < len(equipmentToCheck):
        equipmentToAdd = []
        for eq in equipmentToCheck[i:]:
            if isinstance(eq, cim.PowerTransformer):
                xfmr = eq
                break
            else:
                terminals = eq.Terminals
                connectivityNodes = []
                for t in terminals:
                    if t.ConnectivityNode not in connectivityNodes:
                        connectivityNodes.append(t.ConnectivityNode)
                for cn in connectivityNodes:
                    for t in cn.Terminals:
                        if t.ConductingEquipment not in equipmentToCheck and t.ConductingEquipment not in equipmentToAdd:
                            equipmentToAdd.append(t.ConductingEquipment)
        i = len(equipmentToCheck)
        equipmentToCheck.extend(equipmentToAdd)
    if not xfmr:
        raise RuntimeError('findPrimaryPhase(): no upstream centertapped transformer could be found for secondary '
                           f'system object {cimObj.name}!')
    for tank in xfmr.TransformerTanks:
        if phaseCode is not None:
            break
        for tankEnd in tank.TransformerTankEnds:
            if tankEnd.phases not in [
                cim.PhaseCode.none, cim.PhaseCode.s1, cim.PhaseCode.s12, cim.PhaseCode.s12N, cim.PhaseCode.s1N,
                cim.PhaseCode.s2, cim.PhaseCode.s2N
            ]:
                phaseCode = tankEnd.phases
                break
    if not phaseCode:
        raise RuntimeError('findPrimaryPhase(): the upstream centertapped transformer has no primary phase defined!?')
    return phaseCode


def findFeederPowerRating(cimObj):
    '''
        Helper function for finding the feeder's rated power from and instance of cim.EnergySource.
    '''
    if not isinstance(cimObj, cim.EnergySource):
        raise TypeError('findPrimaryPhase(): cimObj must be an instance of cim.EnergySource!')
    equipmentToCheck = [cimObj]
    xfmr = None
    feederPowerRating = None
    i = 0
    while not xfmr and i < len(equipmentToCheck):
        equipmentToAdd = []
        for eq in equipmentToCheck[i:]:
            if isinstance(eq, cim.PowerTransformer):
                xfmr = eq
                break
            else:
                terminals = eq.Terminals
                connectivityNodes = []
                for t in terminals:
                    if t.ConnectivityNode not in connectivityNodes:
                        connectivityNodes.append(t.ConnectivityNode)
                for cn in connectivityNodes:
                    for t in cn.Terminals:
                        if t.ConductingEquipment not in equipmentToCheck and t.ConductingEquipment not in equipmentToAdd:
                            equipmentToAdd.append(t.ConductingEquipment)
        i = len(equipmentToCheck)
        equipmentToCheck.extend(equipmentToAdd)
    if not xfmr:
        raise RuntimeError('findFeederPowerRating(): No feeder head transformer could be found for EnergySource, '
                           f'{cimObj.name}!')
    powerTransformerEnds = xfmr.PowerTransformerEnd
    if powerTransformerEnds:
        feederPowerRating = float(powerTransformerEnds[0].ratedS)
    else:
        raise RuntimeError('findFeederPowerRating(): The found at the feeder head is not a three phase transformer!')
    logger.info(f'Feeder Power Rating: {feederPowerRating} W')
    return feederPowerRating


def findFeederHeadPowerMeasurmentsObject(cimObj: object):
    '''
        Helper function to find feeder transformer
    '''
    if not isinstance(cimObj, cim.EnergySource):
        raise TypeError('findFeederHeadPowerMeasurmentsObject(): cimObj must be an instance of cim.EnergySource!')
    equipmentToCheck = [cimObj]
    feederHeadLoadMeasurementsObject = None
    i = 0
    while not feederHeadLoadMeasurementsObject and i < len(equipmentToCheck):
        equipmentToAdd = []
        for eq in equipmentToCheck[i:]:
            powerMeasurementsCount = 0
            for meas in eq.Measurements:
                if meas.measurementType == 'VA':
                    powerMeasurementsCount += 1
            if powerMeasurementsCount == 3:
                feederHeadLoadMeasurementsObject = eq
                break
            else:
                terminals = eq.Terminals
                connectivityNodes = []
                for t in terminals:
                    if t.ConnectivityNode not in connectivityNodes:
                        connectivityNodes.append(t.ConnectivityNode)
                for cn in connectivityNodes:
                    for t in cn.Terminals:
                        if t.ConductingEquipment not in equipmentToCheck and t.ConductingEquipment not in equipmentToAdd:
                            equipmentToAdd.append(t.ConductingEquipment)
        i = len(equipmentToCheck)
        equipmentToCheck.extend(equipmentToAdd)
    if not feederHeadLoadMeasurementsObject:
        raise RuntimeError('findFeederPowerRating(): No feeder head object with power measurements could be found for '
                           f'EnergySource, {cimObj.name}!')
    return feederHeadLoadMeasurementsObject


def buildGraphModel(mrid: str) -> FeederModel:
    global CIM_GRAPH_MODELS
    if not isinstance(mrid, str):
        raise TypeError(f'The mrid passed to the convervation voltage reduction application must be a string.')
    if mrid not in CIM_GRAPH_MODELS.keys():
        feeder_container = cim.Feeder(mRID=mrid)
        graph_model = FeederModel(connection=DB_CONNECTION, container=feeder_container, distributed=False)
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
    start = datetime(2023, 1, 1, 0, tzinfo=timezone.utc)
    epoch = datetime.timestamp(start)
    duration = timedelta(minutes=1).total_seconds()
    sim_args = SimulationArgs(start_time=f'{epoch}',
                              duration=f'{duration}',
                              simulation_name=sim_name,
                              run_realtime=True,
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
        os.environ['GRIDAPPSD_APPLICATION_ID'] = 'PeakShavingApplication'
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


def pol2cart(mag, angle_deg):
    # Convert degrees to radians. GridAPPS-D spits angle in degrees
    angle_rad = math.radians(angle_deg)
    p = mag * math.cos(angle_rad)
    q = mag * math.sin(angle_rad)
    return p, q


class PeakShavingController(object):
    """Peak Shaving Control Class
        This class implements a centralized algorithm for feeder peak shaving by controlling batteries in the
        distribution model.

        Attributes:
            sim_id: The simulation id that the instance will be perfoming peak shaving on. If the sim_id is None then the
                    application will assume it is performing peak shaving on the actual field devices.
                Type: str.
                Default: None.
        Methods:
            on_measurement(headers:Dict[Any], message:Dict[Any]): This callback function will be used to update the
                    applications measurements dictionary needed for peak shaving control.
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
                 base_setpoint: float = None,
                 sim_id: str = None,
                 simulation: Simulation = None):
        if not isinstance(gad_obj, GridAPPSD):
            raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
        if not isinstance(model_id, str):
            raise TypeError(f'model_id must be a str type.')
        if model_id is None or model_id == '':
            raise ValueError(f'model_id must be a valid uuid.')
        if not isinstance(peak_setpoint, float) and peak_setpoint is not None:
            raise TypeError(f'peak_setpoint must be of float type or {None}!')
        if not isinstance(base_setpoint, float) and base_setpoint is not None:
            raise TypeError(f'base_setpoint must be of float type or {None}!')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'sim_id must be a string type or {None}!')
        if not isinstance(simulation, Simulation) and simulation is not None:
            raise TypeError(f'The simulation arg must be a Simulation type or {None}!')
        self.model_results_path = None
        self.timestamp = 0
        self.model_id = model_id
        self.id = uuid4()
        self.desired_setpoints = {}
        self.pnv_measurements = {}
        self.pnv_measurements_pu = {}
        self.va_measurements = {}
        self.pos_measurements = {}
        self.link_measurements = {}
        self.controllable_batteries_ABC = {}
        self.controllable_batteries_A = {}
        self.controllable_batteries_B = {}
        self.controllable_batteries_C = {}
        self.peak_va_measurements_A = {}
        self.peak_va_measurements_B = {}
        self.peak_va_measurements_C = {}
        self.peak_setpoint_A = None
        self.peak_setpoint_B = None
        self.peak_setpoint_C = None
        self.base_setpoint_A = None
        self.base_setpoint_B = None
        self.base_setpoint_C = None
        self.measurements_topic = None
        self.setpoints_topic = None
        self.simulation = None
        self.simulation_id = None  # TODO: figure out what simulation_id should be when deployed in the field.
        if simulation is not None:
            self.simulation = simulation
            self.simulation_id = simulation.simulation_id
            self.simulation.add_onmeasurement_callback(self.on_measurement)
        if sim_id is None:
            self.measurements_topic = topics.field_output_topic(None, sim_id)
        else:
            self.simulation_id = sim_id
            self.measurements_topic = topics.simulation_output_topic(sim_id)

        self.gad_obj = gad_obj
        self.log = self.gad_obj.get_logger()
        if self.measurements_topic:
            self.gad_obj.subscribe(self.measurements_topic, self.on_measurement_callback)
        if self.simulation_id:
            self.setpoints_topic = topics.simulation_input_topic(self.simulation_id)
        else:
            self.setpoints_topic = topics.field_input_topic()
        # Read model_id from cimgraph to get all the controllable batteries, and measurements.
        self.graph_model = buildGraphModel(model_id)
        self.installed_battery_capacity_ABC = 0.0
        self.installed_battery_capacity_A = 0.0
        self.installed_battery_capacity_B = 0.0
        self.installed_battery_capacity_C = 0.0
        self.installed_battery_power_ABC = 0.0
        self.installed_battery_power_A = 0.0
        self.installed_battery_power_B = 0.0
        self.installed_battery_power_C = 0.0
        self.configureBatteryProperties()
        # feederLoadMeasurements(measurements.keys())
        if peak_setpoint is not None:
            self.peak_setpoint_A = peak_setpoint / 3.0
            self.peak_setpoint_B = peak_setpoint / 3.0
            self.peak_setpoint_C = peak_setpoint / 3.0
        else:
            self.configurePeakShavingSetpoint()
        if base_setpoint is not None:
            self.base_setpoint_A = base_setpoint / 3.0
            self.base_setpoint_B = base_setpoint / 3.0
            self.base_setpoint_C = base_setpoint / 3.0
        else:
            self.base_setpoint_A = 0.8 * self.peak_setpoint_A
            self.base_setpoint_B = 0.8 * self.peak_setpoint_B
            self.base_setpoint_C = 0.8 * self.peak_setpoint_C
        self.findFeederHeadLoadMeasurements()


        measurements = {}
        measurements.update(self.graph_model.graph.get(cim.Analog, {}))
        measurements.update(self.graph_model.graph.get(cim.Discrete, {}))

        # print(measurements.keys())
        for meas in measurements.values():
            if meas.measurementType == 'PNV':    # it's a voltage measurement. store it.
                mrid = meas.mRID
                self.pnv_measurements[mrid] = {'measurement_object': meas, 'measurement_value': None}
            elif meas.measurementType == 'VA':    # it's a power measurement.
                if isinstance(meas.PowerSystemResource, (cim.EnergyConsumer, cim.PowerElectronicsConnection)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.va_measurements.keys():
                        self.va_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.va_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.va_measurements[mrid]['measurement_values'][meas.mRID] = None
                if isinstance(meas.PowerSystemResource, (cim.ACLineSegment, cim.PowerTransformer)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.va_measurements.keys():
                        self.link_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.link_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.link_measurements[mrid]['measurement_values'][meas.mRID] = None
            elif meas.measurementType == 'Pos':    # it's a positional measurement.
                if isinstance(meas.PowerSystemResource, (cim.Switch, cim.PowerTransformer, cim.LinearShuntCompensator)):
                    mrid = meas.PowerSystemResource.mRID
                    if mrid not in self.pos_measurements.keys():
                        self.pos_measurements[mrid] = {'measurement_objects': {}, 'measurement_values': {}}
                    self.pos_measurements[mrid]['measurement_objects'][meas.mRID] = meas
                    self.pos_measurements[mrid]['measurement_values'][meas.mRID] = None

        # data output
        out_dir = Path(__file__).parent/"output"
        feeder = self.graph_model.graph.get(cim.Feeder, {}).get(self.model_id)
        self.model_results_path = out_dir/feeder.name
        self.model_results_path.mkdir(parents=True, exist_ok=True)
        # self.findFeederHeadLoadMeasurements()
        self.init_output_file()
        if self.simulation is not None:
            self.simulation.start_simulation()
            self.simulation_id = self.simulation.simulation_id
        self.differenceBuilder = DifferenceBuilder(self.simulation_id)
        self.isValid = True
        self.first_message = True

    def init_output_file(self):
        for child in self.model_results_path.iterdir():
            if not child.is_dir():
                child.unlink()
        with open(self.model_results_path / "voltages.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "node", "phase", "voltage"]
            writer.writerow(header)
        with open(self.model_results_path / "total_load.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "phase", "p", "q"]
            writer.writerow(header)
        with open(self.model_results_path / "feeder_head.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "phase", "p", "q"]
            writer.writerow(header)
        with open(self.model_results_path / "battery_power.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "battery", "p_a", "p_b", "p_c", "soc"]
            writer.writerow(header)
        with open(self.model_results_path / "dispatches.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "name", "phase", "value"]
            writer.writerow(header)
        with open(self.model_results_path / "loads_no_batt.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "a", "b", "c"]
            writer.writerow(header)

    def get_feeder_head_measurements(self, measurements):
        total_loads = {}
        if self.graph_model.container.mRID == IEEE123_APPS:
            for mrid in ieee123_apps_feeder_head_measurement_mrids:
                phase = self.graph_model.graph.get(cim.Analog, {}).get(mrid).phases.value
                s_magnitude = measurements.get(mrid).get("magnitude")
                s_angle_rad = measurements.get(mrid).get("angle")
                total_loads[phase] = s_magnitude*exp(1j*math.radians(s_angle_rad))
        return total_loads

    def on_measurement(self, sim: Simulation, timestamp: str, measurements: Dict[str, Dict]):
        self.timestamp = timestamp
        self.desired_setpoints.clear()
        if isinstance(sim, Simulation):
            self.setpoints_topic = topics.simulation_input_topic(sim.simulation_id)
            sim.pause()
        total_loads = self.get_feeder_head_measurements(measurements)
        self.save_total_load_data(timestamp, total_loads)
        for mrid in self.peak_va_measurements_A.keys():
            measurement = measurements.get(self.peak_va_measurements_A[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_A[mrid]['value'] = measurement
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load = mag * exp(1j*math.radians(ang_in_deg))
                    add_data_to_csv(self.model_results_path/"feeder_head.csv", [self.timestamp, mrid, "A", load.real, load.imag])
        for mrid in self.peak_va_measurements_B.keys():
            measurement = measurements.get(self.peak_va_measurements_B[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_B[mrid]['value'] = measurement
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load = mag * exp(1j*math.radians(ang_in_deg))
                    add_data_to_csv(self.model_results_path/"feeder_head.csv", [self.timestamp, mrid, "B", load.real, load.imag])
        for mrid in self.peak_va_measurements_C.keys():
            measurement = measurements.get(self.peak_va_measurements_C[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_C[mrid]['value'] = measurement
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load = mag * exp(1j*math.radians(ang_in_deg))
                    add_data_to_csv(self.model_results_path/"feeder_head.csv", [self.timestamp, mrid, "C", load.real, load.imag])
        for mrid in self.controllable_batteries_ABC.keys():
            power_data_to_save = [0, 0, 0]
            for measDict in self.controllable_batteries_ABC[mrid]['power_measurements']:
                measurement = measurements.get(measDict['object'].mRID)
                if measurement is not None:
                    measDict['value'] = measurement
                    phase = measDict["object"].phases.value
                    p, q = pol2cart(measurement.get("magnitude"), measurement.get("angle"))
                    power_data_to_save[("ABC").index(phase)] = p
            measurement = measurements.get(self.controllable_batteries_ABC[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_ABC[mrid]['soc_measurement']['value'] = measurement
            header = ["time", "mrid", "battery", "p_a", "p_b", "p_c", "soc"]
            data = [self.timestamp, mrid, measDict['object'].name]
            data.extend(power_data_to_save)
            data.append(self.controllable_batteries_ABC[mrid]['soc_measurement']['value'].get("value"))
            add_data_to_csv(self.model_results_path/"battery_power.csv", data)
        for mrid in self.controllable_batteries_A.keys():
            for measDict in self.controllable_batteries_A[mrid]['power_measurements']:
                measurement = measurements.get(measDict['object'].mRID)
                if measurement is not None:
                    measDict['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_A[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_A[mrid]['soc_measurement']['value'] = measurement
        for mrid in self.controllable_batteries_B.keys():
            for measDict in self.controllable_batteries_B[mrid]['power_measurements']:
                measurement = measurements.get(measDict['object'].mRID)
                if measurement is not None:
                    measDict['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_B[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_B[mrid]['soc_measurement']['value'] = measurement
        for mrid in self.controllable_batteries_C.keys():
            for measDict in self.controllable_batteries_C[mrid]['power_measurements']:
                measurement = measurements.get(measDict['object'].mRID)
                if measurement is not None:
                    measDict['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_C[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_C[mrid]['soc_measurement']['value'] = measurement

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
                    if isinstance(self.pos_measurements[psr_mrid]['measurement_objects'][mrid].PowerSystemResource, (cim.LinearShuntCompensator, cim.PowerTransformer)):
                        pos_measurement_debug_data = {
                            "object_type": f"{type(self.pos_measurements[psr_mrid]['measurement_objects'][mrid].PowerSystemResource).__name__}",
                            "name": self.pos_measurements[psr_mrid]['measurement_objects'][mrid].PowerSystemResource.name,
                            "phase": self.pos_measurements[psr_mrid]['measurement_objects'][mrid].phases.value,
                            "value": int(meas.get("value")),
                            "timestamp": timestamp
                        }
                        logger.info(f"Pos Measurement: {json.dumps(pos_measurement_debug_data)}")

        self.calculate_per_unit_voltage()
        self.save_voltage_data(timestamp)
        self.peak_shaving_control()
        if isinstance(sim, Simulation):
            sim.resume()

    def findFeederHeadLoadMeasurements(self):
        energySources = self.graph_model.graph.get(cim.EnergySource, {})
        feederLoadObject = None
        for eSource in energySources.values():
            feederLoadObject = findFeederHeadPowerMeasurmentsObject(eSource)
            feederLoadMeasurements = feederLoadObject.Measurements
            for measurement in feederLoadMeasurements:
                if measurement.measurementType == 'VA':
                    if measurement.phases in [cim.PhaseCode.A, cim.PhaseCode.AN]:
                        self.peak_va_measurements_A[measurement.mRID] = {'object': measurement, 'value': None}
                    elif measurement.phases in [cim.PhaseCode.B, cim.PhaseCode.BN]:
                        self.peak_va_measurements_B[measurement.mRID] = {'object': measurement, 'value': None}
                    elif measurement.phases in [cim.PhaseCode.C, cim.PhaseCode.CN]:
                        self.peak_va_measurements_C[measurement.mRID] = {'object': measurement, 'value': None}
        if not self.peak_va_measurements_A or not self.peak_va_measurements_B or not self.peak_va_measurements_C:
            raise RuntimeError(f'feeder {self.graph_model.container.mRID}, has no measurements associated with the '
                               'feeder head transformer!')

    def calculate_per_unit_voltage(self):
        for mrid in self.pnv_measurements.keys():
            self.pnv_measurements_pu[mrid] = None
            meas_base = None
            meas = self.pnv_measurements.get(mrid)
            if (meas is None) or (meas.get('measurement_value') is None):
                self.log.warning(f'Measurement {mrid} is missing. It will be ignored in cvr control.')
                continue
            meas_value = meas.get('measurement_value').get('magnitude')
            if meas_value is None:
                self.log.error(f'An unexpected value of None was recieved for PNV measurement {mrid}. It will be '
                               'ignored in cvr control.')
                continue
            meas_obj = meas.get('measurement_object')
            if (meas_obj is None) or (not isinstance(meas_obj, cim.Measurement)):
                self.log.error(f'The cim.Measurement object {mrid} associated with this measurement value is missing! '
                               'It will be ignored in cvr control.')
                logger.error(f'The measurement dictionary for mrid {mrid} is missing from the CIM database.')
                continue

            meas_term = meas_obj.Terminal
            if isinstance(meas_obj.PowerSystemResource, cim.PowerElectronicsConnection):
                meas_base = float(meas_obj.PowerSystemResource.ratedU)
            elif isinstance(meas_term, cim.Terminal):
                if meas_term.TransformerEnd:
                    if isinstance(meas_term.TransformerEnd[0], cim.PowerTransformerEnd):
                        meas_base = float(meas_term.TransformerEnd[0].ratedU)
                    elif isinstance(meas_term.TransformerEnd[0], cim.TransformerTankEnd):
                        if isinstance(meas_term.TranformerEnd[0].BaseVoltage, cim.BaseVoltage):
                            meas_base = float(meas_term.TransformerEnd[0].BaseVoltage.nominalVoltage)
                        else:
                            self.log.error('meas_term.TransformerEnd[0].BaseVoltage is None')
                            logger.error('meas_term.TransformerEnd[0].BaseVoltage is None')
                            raise RuntimeError('PNV measurement BaseVoltage for ConnectivityNode '
                                            f'{meas_term.ConnectivityNode.name} is None')
                elif isinstance(meas_term.ConductingEquipment, cim.ConductingEquipment):
                    if isinstance(meas_term.ConductingEquipment.BaseVoltage, cim.BaseVoltage):
                        meas_base = float(meas_term.ConductingEquipment.BaseVoltage.nominalVoltage)
                    else:
                        logger.error(f'meas_term.ConductingEquipment.BaseVoltage is None')
                else:
                    logger.error(f'meas_term.ConductingEquipment is None')
            if (meas_base is None) or (meas_base < 1e-10):
                self.log.error(f'Unable to get the nominal voltage for measurement with mrid {mrid}.')
                logger.error('Voltage Measurement has no associated nominal Voltage.\nMeasurement: '
                             f'{meas_obj.name}\nTerminal: {meas_obj.Terminal.name}\n')
                continue
            pnv_meta_data = {'measured PNV': meas_value, 'measured LLV': meas_value * math.sqrt(3.0), 'rated V': meas_base}
            pu_vals = [meas_value / meas_base, meas_value * math.sqrt(3.0) / meas_base]
            diff = max(pu_vals)
            for i in range(len(pu_vals)):
                if abs(1.0-pu_vals[i]) < diff:
                    diff = abs(1.0-pu_vals[i])
                    self.pnv_measurements_pu[mrid] = pu_vals[i]
            pnv_meta_data['pu V'] = self.pnv_measurements_pu[mrid]
            logger.info(f'Voltage meta data for {meas_obj.Terminal.ConnectivityNode.name}:{json.dumps(pnv_meta_data,indent=4)}')

    def save_total_load_data(self, time, total_loads):
        file_path = self.model_results_path/"total_load.csv"
        header = ["time", "phase", "p", "q"]
        for phase, load in total_loads.items():
            data = [time, phase, load.real, load.imag]
            add_data_to_csv(file_path, data, header=header)

    def save_voltage_data(self, time):
        file_path = self.model_results_path/"voltages.csv"
        header = ["time", "mrid", "node", "phase", "voltage"]
        for mrid in self.pnv_measurements.keys():
            v = self.pnv_measurements_pu[mrid]
            phase = self.pnv_measurements[mrid]["measurement_object"].phases.value
            node = self.pnv_measurements[mrid]["measurement_object"].Terminal.ConnectivityNode.name
            if v is not None:
                data = [time, mrid, node, phase, v]
                add_data_to_csv(file_path, data, header=header)

    def configureBatteryProperties(self):
        '''This function uses cimgraph to populate the following properties:

            self.controllable_batteries_A
            self.controllable_batteries_B
            self.controllable_batteries_C
        '''
        powerElectronicsConnections = self.graph_model.graph.get(cim.PowerElectronicsConnection, {})
        for pec in powerElectronicsConnections.values():
            isBatteryInverter = False
            batteryCapacity = 0.0
            for powerElectronicsUnit in pec.PowerElectronicsUnit:
                if isinstance(powerElectronicsUnit, cim.BatteryUnit):
                    isBatteryInverter = True
                    batteryCapacity += float(powerElectronicsUnit.ratedE)
            if isBatteryInverter:
                inverterPhases = self.getInverterPhases(pec)
                if inverterPhases in [cim.PhaseCode.A, cim.PhaseCode.AN]:
                    self.installed_battery_capacity_A += batteryCapacity
                    self.installed_battery_power_A += float(pec.ratedS)
                    self.controllable_batteries_A[pec.mRID] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurements': findMeasurements(pec.Measurements, 'VA'),
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.none]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.B, cim.PhaseCode.BN]:
                    self.installed_battery_capacity_B += batteryCapacity
                    self.installed_battery_power_B += float(pec.ratedS)
                    self.controllable_batteries_B[pec.mRID] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurements': findMeasurements(pec.Measurements, 'VA'),
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.C, cim.PhaseCode.CN]:
                    self.installed_battery_capacity_C += batteryCapacity
                    self.installed_battery_power_C += float(pec.ratedS)
                    self.controllable_batteries_C[pec.mRID] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurements': findMeasurements(pec.Measurements, 'VA'),
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.ABC, cim.PhaseCode.ABCN]:
                    self.installed_battery_capacity_ABC += batteryCapacity
                    self.installed_battery_power_ABC += float(pec.ratedS)
                    self.controllable_batteries_ABC[pec.mRID] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurements': findMeasurements(pec.Measurements, 'VA'),
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.none]),
                            'value': None
                        }
                    }

    def getInverterPhases(self, cimObj):
        # algorithm that attempts to find the upstream centertapped transformer feeding secondary system inverters.
        phaseCode = None
        if not isinstance(cimObj, cim.PowerElectronicsConnection):
            raise TypeError('PeakShavingController.getInverterPhases(): cimObj must be an instance of '
                            'cim.PowerElectronicsConnection!')
        strPhases = ''
        if len(cimObj.PowerElectronicsConnectionPhases) > 0:
            for pecp in cimObj.PowerElectronicsConnectionPhases:
                if pecp.phase != cim.SinglePhaseKind.s1 and pecp.phase != cim.SinglePhaseKind.s2:
                    strPhases += pecp.phase.value
                else:
                    strPhases = pecp.phase.value
        else:
            strPhases = 'ABC'
        if 'A' in strPhases or 'B' in strPhases or 'C' in strPhases:
            phaseCodeStr = ''
            if 'A' in strPhases:
                phaseCodeStr += 'A'
            if 'B' in strPhases:
                phaseCodeStr += 'B'
            if 'C' in strPhases:
                phaseCodeStr += 'C'
            phaseCode = cim.PhaseCode(phaseCodeStr)
        else:  # inverter is on the secondary system need to trace up to the centertapped tansformer.
            phaseCode = self.findPrimaryPhase(cimObj)
        return phaseCode

    def configurePeakShavingSetpoint(self):
        energySources = self.graph_model.graph.get(cim.EnergySource, {})
        feederPowerRating = 0.0
        for source in energySources.values():
            feederPowerRating = findFeederPowerRating(source)
        self.peak_setpoint_A = max(0.5 * (feederPowerRating / 3.0),
                                   (feederPowerRating / 3.0) - (self.installed_battery_capacity_A * 0.95))
        self.peak_setpoint_B = max(0.5 * (feederPowerRating / 3.0),
                                   (feederPowerRating / 3.0) - (self.installed_battery_capacity_B * 0.95))
        self.peak_setpoint_C = max(0.5 * (feederPowerRating / 3.0),
                                   (feederPowerRating / 3.0) - (self.installed_battery_capacity_C * 0.95))

    def on_measurement_callback(self, header: Dict[str, Any], message: Dict[str, Any]):
        timestamp = message.get('message', {}).get('timestamp', '')
        measurements = message.get('message', {}).get('measurements', {})
        self.on_measurement(None, timestamp, measurements)

    def peak_shaving_control(self):
        lower_limit = 20.0
        upper_limit = 80.0
        real_load_A, real_load_B, real_load_C = self.get_load_minus_batteries()
        add_data_to_csv(self.model_results_path/"loads_no_batt.csv", [self.timestamp, real_load_A, real_load_B, real_load_C])
        power_diff_A = 0.0
        power_diff_B = 0.0
        power_diff_C = 0.0
        deadband_ABC = True
        if real_load_A > self.peak_setpoint_A:
            power_diff_A = real_load_A - self.peak_setpoint_A
        elif real_load_A < self.base_setpoint_A:
            power_diff_A = real_load_A - self.base_setpoint_A
        if real_load_B > self.peak_setpoint_B:
            power_diff_B = real_load_B - self.peak_setpoint_B
        elif real_load_B < self.base_setpoint_B:
            power_diff_B = real_load_B - self.base_setpoint_B
        if real_load_C > self.peak_setpoint_C:
            power_diff_C = real_load_C - self.peak_setpoint_C
        elif real_load_C < self.base_setpoint_C:
            power_diff_C = real_load_C - self.base_setpoint_C
        min_power_diff = min(power_diff_A, power_diff_B, power_diff_C)
        max_power_diff = max(power_diff_A, power_diff_B, power_diff_C)
        if min_power_diff > 1e-6:
            deadband_ABC = False
            control_dict, actual_power = self.calc_batt_discharge_ABC(3.0 * min_power_diff, lower_limit)
            self.desired_setpoints.update(control_dict)
            power_diff_A -= actual_power / 3.0
            power_diff_B -= actual_power / 3.0
            power_diff_C -= actual_power / 3.0
        elif max_power_diff < -1e-6:
            deadband_ABC = False
            control_dict, actual_power = self.calc_batt_charge_ABC(3.0 * abs(max_power_diff), upper_limit)
            self.desired_setpoints.update(control_dict)
            power_diff_A += abs(actual_power) / 3.0
            power_diff_B += abs(actual_power) / 3.0
            power_diff_C += abs(actual_power) / 3.0
        if power_diff_A > 1e-6:
            control_dict = self.calc_batt_discharge_A(power_diff_A, lower_limit)
            self.desired_setpoints.update(control_dict)
        elif power_diff_A < -1e-6:
            control_dict = self.calc_batt_charge_A(abs(power_diff_A), upper_limit)
            self.desired_setpoints.update(control_dict)
        if power_diff_B > 1e-6:
            control_dict = self.calc_batt_discharge_B(power_diff_B, lower_limit)
            self.desired_setpoints.update(control_dict)
        elif power_diff_B < -1e-6:
            control_dict = self.calc_batt_charge_B(abs(power_diff_B), upper_limit)
            self.desired_setpoints.update(control_dict)
        if power_diff_C > 1e-6:
            control_dict = self.calc_batt_discharge_C(power_diff_C, lower_limit)
            self.desired_setpoints.update(control_dict)
        elif power_diff_C < -1e-6:
            control_dict = self.calc_batt_charge_C(abs(power_diff_C), upper_limit)
            self.desired_setpoints.update(control_dict)
        if deadband_ABC:
            for batt_id in self.controllable_batteries_ABC.keys():
                measurements = self.controllable_batteries_ABC[batt_id]['power_measurements']
                mag = []
                ang_in_deg = []
                for measurement in measurements:
                    if measurement.get('value') is None:
                        break
                    mag.append(measurement.get('value', {}).get('magnitude'))
                    ang_in_deg.append(measurement.get('value', {}).get('angle'))
                if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                    continue
                current_power = 0.0
                for i in range(len(mag)):
                    current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
                if (abs(current_power) > 1e-6):
                    self.desired_setpoints[batt_id] = {
                        'object': self.controllable_batteries_ABC[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
        if self.desired_setpoints:
            self.send_setpoints()

    def get_load_minus_batteries(self):
        load_A = 0.0
        load_B = 0.0
        load_C = 0.0
        for xfmr_id in self.peak_va_measurements_A.keys():
            measurement = self.peak_va_measurements_A[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_A += mag * math.cos(math.radians(ang_in_deg))
        for xfmr_id in self.peak_va_measurements_B.keys():
            measurement = self.peak_va_measurements_B[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_B += mag * math.cos(math.radians(ang_in_deg))
        for xfmr_id in self.peak_va_measurements_C.keys():
            measurement = self.peak_va_measurements_C[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_C += mag * math.cos(math.radians(ang_in_deg))
        for batt_id_3ph in self.controllable_batteries_ABC.keys():
            for measDict in self.controllable_batteries_ABC[batt_id_3ph]['power_measurements']:
                measurement = measDict.get('value')
                measurementObj = measDict.get('object')
                if (measurement is None) or (measurementObj is None):
                    continue
                measurementPhase = measurementObj.phases
                if measurementPhase in [cim.PhaseCode.A, cim.PhaseCode.AN]:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_A -= mag * math.cos(math.radians(ang_in_deg))
                elif measurementPhase in [cim.PhaseCode.B, cim.PhaseCode.BN]:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_B -= mag * math.cos(math.radians(ang_in_deg))
                elif measurementPhase in [cim.PhaseCode.C, cim.PhaseCode.CN]:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_C -= mag * math.cos(math.radians(ang_in_deg))
        for batt_id_1ph in self.controllable_batteries_A.keys():
            for measDict in self.controllable_batteries_A[batt_id_1ph]['power_measurements']:
                measurement = measDict.get('value')
                if measurement is not None:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_A -= mag * math.cos(math.radians(ang_in_deg))
        for batt_id_1ph in self.controllable_batteries_B.keys():
            for measDict in self.controllable_batteries_B[batt_id_1ph]['power_measurements']:
                measurement = measDict.get('value')
                if measurement is not None:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_B -= mag * math.cos(math.radians(ang_in_deg))
        for batt_id_1ph in self.controllable_batteries_C.keys():
            for measDict in self.controllable_batteries_C[batt_id_1ph]['power_measurements']:
                measurement = measDict.get('value')
                if measurement is not None:
                    mag = measurement.get('magnitude')
                    ang_in_deg = measurement.get('angle')
                    if ((mag is not None) and (ang_in_deg is not None)):
                        load_C -= mag * math.cos(math.radians(ang_in_deg))
        return (load_A, load_B, load_C)

    def calc_batt_discharge_ABC(self, power_to_discharge_ABC, lower_limit):
        if not isinstance(power_to_discharge_ABC, float):
            raise TypeError('calc_batt_discharge_ABC(): power_to_discharge_ABC must be an instance of float!')
        if power_to_discharge_ABC < 0.0:
            raise ValueError(f'calc_batt_discharge_ABC(): power_to_discharge_ABC must be nonnegative!')
        if not isinstance(lower_limit, float):
            raise TypeError('calc_batt_discharge_ABC(): lower_limit must be an instance of float!')
        if (lower_limit > 100.0) or (lower_limit < 0.0):
            raise ValueError(f'calc_batt_discharge_ABC(): lower_limit must belong to the [0, 1] interval!')
        return_dict = {}
        power_acc = 0.0
        available_capacity_ABC = self.installed_battery_power_ABC
        for batt_id in self.controllable_batteries_ABC.keys():
            measurements = self.controllable_batteries_ABC[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_ABC[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc < lower_limit:
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
            else:
                available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
        if available_capacity_ABC <= 0.0:
            return return_dict, power_acc
        for batt_id in self.controllable_batteries_ABC.keys():
            measurements = self.controllable_batteries_ABC[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    break
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_ABC[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc < lower_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_ABC[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_discharge_ABC /
                         available_capacity_ABC) * self.controllable_batteries_ABC[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_ABC[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1.0e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_ABC[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': new_power
                }
                power_acc += new_power
            else:
                power_acc += current_power
        return return_dict, power_acc

    def calc_batt_charge_ABC(self, power_to_charge_ABC, upper_limit):
        if not isinstance(power_to_charge_ABC, float):
            raise TypeError('calc_batt_charge_ABC(): power_to_charge_ABC must be an instance of float!')
        if power_to_charge_ABC < 0.0:
            raise ValueError(f'calc_batt_charge_ABC(): power_to_charge_ABC must be nonnegative!')
        if not isinstance(upper_limit, float):
            raise TypeError('calc_batt_charge_ABC(): upper_limit must be an instance of float!')
        if (upper_limit > 100.0) or (upper_limit < 0.0):
            raise ValueError(f'calc_batt_charge_ABC(): upper_limit must belong to the [0, 1] interval!')
        return_dict = {}
        power_acc = 0.0
        available_capacity_ABC = self.installed_battery_power_ABC
        for batt_id in self.controllable_batteries_ABC.keys():
            measurements = self.controllable_batteries_ABC[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_ABC[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
            else:
                available_capacity_ABC -= self.controllable_batteries_ABC[batt_id]['maximum_power']
        if available_capacity_ABC <= 0.0:
            return return_dict, power_acc
        for batt_id in self.controllable_batteries_ABC.keys():
            measurements = self.controllable_batteries_ABC[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    break
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_ABC[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc > upper_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_ABC[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_charge_ABC /
                         available_capacity_ABC) * self.controllable_batteries_ABC[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_ABC[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_ABC[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': -new_power
                }
                power_acc += new_power
            else:
                power_acc += current_power
        return return_dict, power_acc

    def calc_batt_discharge_A(self, power_to_discharge_A, lower_limit):
        if not isinstance(power_to_discharge_A, float):
            raise TypeError('calc_batt_discharge_A(): power_to_discharge_A must be an instance of float!')
        if power_to_discharge_A < 0.0:
            raise ValueError(f'calc_batt_discharge_A(): power_to_discharge_A must be nonnegative!')
        if not isinstance(lower_limit, float):
            raise TypeError('calc_batt_discharge_A(): lower_limit must be an instance of float!')
        if (lower_limit > 100.0) or (lower_limit < 0.0):
            raise ValueError(f'calc_batt_discharge_A(): lower_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_A = self.installed_battery_power_A
        for batt_id in self.controllable_batteries_A.keys():
            measurements = self.controllable_batteries_A[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc < lower_limit:
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
            else:
                available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
        if available_capacity_A <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_A.keys():
            measurements = self.controllable_batteries_A[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc < lower_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_A[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_discharge_A /
                         available_capacity_A) * self.controllable_batteries_A[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_A[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_A[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': new_power
                }
        return return_dict

    def calc_batt_charge_A(self, power_to_charge_A, upper_limit):
        if not isinstance(power_to_charge_A, float):
            raise TypeError('calc_batt_charge_A(): power_to_charge_A must be an instance of float!')
        if power_to_charge_A < 0.0:
            raise ValueError(f'calc_batt_charge_A(): power_to_charge_A must be nonnegative!')
        if not isinstance(upper_limit, float):
            raise TypeError('calc_batt_charge_A(): upper_limit must be an instance of float!')
        if (upper_limit > 100.0) or (upper_limit < 0.0):
            raise ValueError(f'calc_batt_charge_A(): upper_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_A = self.installed_battery_power_A
        for batt_id in self.controllable_batteries_A.keys():
            measurements = self.controllable_batteries_A[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
            else:
                available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
        if available_capacity_A <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_A.keys():
            measurements = self.controllable_batteries_A[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc > upper_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_A[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_charge_A /
                         available_capacity_A) * self.controllable_batteries_A[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_A[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_A[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': -new_power
                }
        return return_dict

    def calc_batt_discharge_B(self, power_to_discharge_B, lower_limit):
        if not isinstance(power_to_discharge_B, float):
            raise TypeError('calc_batt_discharge_B(): power_to_discharge_B must be an instance of float!')
        if power_to_discharge_B < 0.0:
            raise ValueError(f'calc_batt_discharge_B(): power_to_discharge_B must be nonnegative!')
        if not isinstance(lower_limit, float):
            raise TypeError('calc_batt_discharge_B(): lower_limit must be an instance of float!')
        if (lower_limit > 100.0) or (lower_limit < 0.0):
            raise ValueError(f'calc_batt_discharge_B(): lower_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_B = self.installed_battery_power_B
        for batt_id in self.controllable_batteries_B.keys():
            measurements = self.controllable_batteries_B[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc < lower_limit:
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
            else:
                available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
        if available_capacity_B <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_B.keys():
            measurements = self.controllable_batteries_B[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc < lower_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_B[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_discharge_B /
                         available_capacity_B) * self.controllable_batteries_B[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_B[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_B[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': new_power
                }
        return return_dict

    def calc_batt_charge_B(self, power_to_charge_B, upper_limit):
        if not isinstance(power_to_charge_B, float):
            raise TypeError('calc_batt_charge_B(): power_to_charge_B must be an instance of float!')
        if power_to_charge_B < 0.0:
            raise ValueError(f'calc_batt_charge_B(): power_to_charge_B must be nonnegative!')
        if not isinstance(upper_limit, float):
            raise TypeError('calc_batt_charge_B(): upper_limit must be an instance of float!')
        if (upper_limit > 100.0) or (upper_limit < 0.0):
            raise ValueError(f'calc_batt_charge_B(): upper_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_B = self.installed_battery_power_B
        for batt_id in self.controllable_batteries_B.keys():
            measurements = self.controllable_batteries_B[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
            else:
                available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
        if available_capacity_B <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_B.keys():
            measurements = self.controllable_batteries_B[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc > upper_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_B[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_charge_B /
                         available_capacity_B) * self.controllable_batteries_B[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_B[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_B[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': -new_power
                }
        return return_dict

    def calc_batt_discharge_C(self, power_to_discharge_C, lower_limit):
        if not isinstance(power_to_discharge_C, float):
            raise TypeError('calc_batt_discharge_C(): power_to_discharge_C must be an instance of float!')
        if power_to_discharge_C < 0.0:
            raise ValueError(f'calc_batt_discharge_C(): power_to_discharge_C must be nonnegative!')
        if not isinstance(lower_limit, float):
            raise TypeError('calc_batt_discharge_C(): lower_limit must be an instance of float!')
        if (lower_limit > 100.0) or (lower_limit < 0.0):
            raise ValueError(f'calc_batt_discharge_C(): lower_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_C = self.installed_battery_power_C
        for batt_id in self.controllable_batteries_C.keys():
            measurements = self.controllable_batteries_C[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc < lower_limit:
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
            else:
                available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
        if available_capacity_C <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_C.keys():
            measurements = self.controllable_batteries_C[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc < lower_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_C[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_discharge_C /
                         available_capacity_C) * self.controllable_batteries_C[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_C[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_C[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': new_power
                }
        return return_dict

    def calc_batt_charge_C(self, power_to_charge_C, upper_limit):
        if not isinstance(power_to_charge_C, float):
            raise TypeError('calc_batt_charge_C(): power_to_charge_C must be an instance of float!')
        if power_to_charge_C < 0.0:
            raise ValueError(f'calc_batt_charge_C(): power_to_charge_C must be nonnegative!')
        if not isinstance(upper_limit, float):
            raise TypeError('calc_batt_charge_C(): upper_limit must be an instance of float!')
        if (upper_limit > 100.0) or (upper_limit < 0.0):
            raise ValueError(f'calc_batt_charge_C(): upper_limit must belong to the [0, 1] interval!')
        return_dict = {}
        available_capacity_C = self.installed_battery_power_C
        for batt_id in self.controllable_batteries_C.keys():
            measurements = self.controllable_batteries_C[batt_id]['power_measurements']
            mag = None
            ang_in_deg = None
            for measurement in measurements:
                if measurement.get('value') is None:
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
                    break
                mag = measurement.get('value', {}).get('magnitude')
                ang_in_deg = measurement.get('value', {}).get('angle')
                if (mag is None) or (ang_in_deg is None):
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
                    break
            if (mag is None) or (ang_in_deg is None):
                continue
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
            else:
                available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
        if available_capacity_C <= 0.0:
            return return_dict
        for batt_id in self.controllable_batteries_C.keys():
            measurements = self.controllable_batteries_C[batt_id]['power_measurements']
            mag = []
            ang_in_deg = []
            for measurement in measurements:
                if measurement.get('value') is None:
                    continue
                mag.append(measurement.get('value', {}).get('magnitude'))
                ang_in_deg.append(measurement.get('value', {}).get('angle'))
            if (not mag) or (not ang_in_deg) or (None in mag) or (None in ang_in_deg):
                continue
            current_power = 0.0
            for i in range(len(mag)):
                current_power += mag[i] * math.cos(math.radians(ang_in_deg[i]))
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value', {}).get('value')
            if batt_soc is not None:
                if (batt_soc > upper_limit) and (abs(current_power) > 1e-6):
                    return_dict[batt_id] = {
                        'object': self.controllable_batteries_C[batt_id]['object'],
                        'old_setpoint': current_power,
                        'setpoint': 0.0
                    }
                    continue
            else:
                continue
            new_power = (power_to_charge_C /
                         available_capacity_C) * self.controllable_batteries_C[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_C[batt_id]['maximum_power'])
            if abs(new_power - current_power) > 1e-6:
                return_dict[batt_id] = {
                    'object': self.controllable_batteries_C[batt_id]['object'],
                    'old_setpoint': current_power,
                    'setpoint': -new_power
                }
        return return_dict

    def send_setpoints(self):
        self.differenceBuilder.clear()
        for dictVal in self.desired_setpoints.values():
            cimObj = dictVal.get('object')
            newSetpoint = dictVal.get('setpoint')
            oldSetpoint = dictVal.get('old_setpoint')
            if isinstance(cimObj, cim.PowerElectronicsConnection):
                self.differenceBuilder.add_difference(cimObj.PowerElectronicsUnit[0].mRID,
                                                      'PowerElectronicsConnection.p', newSetpoint, oldSetpoint)
                name = cimObj.Terminals[0].ConnectivityNode.name
                data = [self.timestamp, cimObj.PowerElectronicsUnit[0].mRID, name, "ABC", newSetpoint]
                add_data_to_csv(self.model_results_path/"dispatches.csv", data)
            else:
                self.log.warning(
                    f'The CIM object with mRID, {cimObj.mRID}, is not a cim.PowerElectronicsConnection. The '
                    f'object is a {type(cimObj)}. This application will ignore sending a setpoint to this '
                    'object.')
        setpointMessage = json.dumps(self.differenceBuilder.get_message(), indent=4, sort_keys=True)
        self.gad_obj.send(self.setpoints_topic, setpointMessage)

    def simulation_completed(self, sim: Simulation):
        self.log.info(f'Simulation for PeakShavingController:{self.id} has finished. This application '
                      'instance can be deleted.')
        self.isValid = False


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
        # TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
        pass
    # Create an peak shaving controller instance for all the real systems in the database
    # for m in models:
    #     m_id = m.get('modelId')
    #     app_instances['field_instances'][m_id] = PeakShavingController(gad_object, m_id)
    for sim_id, m_id in platform_simulations.items():
        app_instances['external_simulation_instances'][sim_id] = PeakShavingController(
            gad_object,
            m_id,
            sim_id=sim_id,
            peak_setpoint = 0.35e6*3,
            base_setpoint = 0.2e6*3,
        )
    for m_id, simulation in local_simulations.items():
        app_instances['local_simulation_instances'][m_id] = PeakShavingController(
            gad_object,
            m_id,
            peak_setpoint = 0.35e6*3,
            base_setpoint = 0.2e6*3,
            simulation=simulation
        )
    app_instances_exist = False
    if len(app_instances['field_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['external_simulation_instances']) > 0:
        app_instances_exist = True
    elif len(app_instances['local_simulation_instances']) > 0:
        app_instances_exist = True
    application_uptime = int(time.time())
    if app_instances_exist:
        gad_log.info('PeakShavingApplication successfully started.')
        gad_object.set_application_status(ProcessStatusEnum.RUNNING)

    while gad_object.connected:
        try:
            app_instances_exist = False
            # TODO: check platform for any running external simulations to control.
            invalidInstances = []
            for m_id, app in app_instances.get('field_instances', {}).items():
                if not app.isValid:
                    invalidInstances.append(m_id)
            for m_id in invalidInstances:
                del app_instances['field_instances'][m_id]
            invalidInstances = []
            for sim_id, app in app_instances.get('external_simulation_instances', {}).items():
                # TODO: check if external simulation has finished and shutdown the corresponding app instance.
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
                    gad_log.info('There have been no running instances PeakShavingController '
                                 'instances for an hour. Shutting down the application.')
                    gad_object.set_application_status(ProcessStatusEnum.STOPPING)
                    gad_object.disconnect()
        except KeyboardInterrupt:
            gad_log.info('Manually exiting PeakShavingApplication')
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
    parser.add_argument('model_id',
                        nargs='?',
                        default=None,
                        help='The mrid of the cim model to perform peak shaving on.')
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
