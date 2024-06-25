import time
import os
import math
from pprint import pprint
# import networkx as nx
# import numpy as np
import cvxpy as cp
# from pandas import DataFrame
from typing import Any, Dict
import importlib
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from argparse import ArgumentParser
import json
from typing import Dict
from cimgraph import utils
from cimgraph.databases import ConnectionParameters, BlazegraphConnection
from cimgraph.models import FeederModel
from cimgraph.data_profile.rc4_2021 import EnergyConsumer
from cimgraph.data_profile.rc4_2021 import BatteryUnit
from cimgraph.data_profile.rc4_2021 import PowerElectronicsConnection
from gridappsd import GridAPPSD, topics, DifferenceBuilder
from gridappsd.simulation import Simulation
from gridappsd.simulation import PowerSystemConfig
from gridappsd.simulation import SimulationConfig
from gridappsd.simulation import SimulationArgs
from gridappsd.simulation import ModelCreationConfig
from gridappsd import topics as t

IEEE123_APPS = "_E3D03A27-B988-4D79-BFAB-F9D37FB289F7"
IEEE13 = "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"
IEEE123 = "_C1C3E687-6FFD-C753-582B-632A27E28507"
IEEE123PV = "_A3BC35AA-01F6-478E-A7B1-8EA4598A685C"
SOURCE_BUS = '150r'
OUT_DIR = "outputs"
ROOT = os.getcwd()



class CarbonManagementApp(object):
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
    period = 3600
    lower_voltage_limit_pu = 0.9
    max_violation_time = 300

    def __init__(self,
                 gad_obj: GridAPPSD,
                 model_id: str,
                 network: FeederModel,
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
        if not isinstance(low_volt_lim, float) and period is not None:
            raise TypeError(f'low_volt_lim must be an float type or {None}!')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'sim_id must be a string type or {None}!')
        if not isinstance(simulation, Simulation) and simulation is not None:
            raise TypeError(f'The simulation arg must be a Simulation type or {None}!')
        self.platform_measurements = {}
        self.last_setpoints = {}
        self.desired_setpoints = {}
        self.controllable_regulators = {}
        self.controllable_capacitors = {}
        self.pnv_measurements = {}
        self.pnv_measurements_pu = {}
        self.va_measurements = {}
        self.pos_measurements = {}
        self.measurements_topic = None
        self.simulation = None
        self.gad_obj = gad_obj
        self.init_batt_dis = True
        self._count = 0
        self._publish_to_topic = topics.simulation_input_topic(simulation.simulation_id)
        self._init_batt_diff = DifferenceBuilder(simulation.simulation_id)

        self.Battery = {}
        self.Solar = {}
        self.EnergyConsumer = {}
        for pec in network.graph[PowerElectronicsConnection].values():
            # inv_mrid = pec.mRID
            for unit in pec.PowerElectronicsUnit:
                unit_mrid = unit.mRID
                if isinstance(unit, BatteryUnit):
                    self.Battery[unit_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [], 'measurementPhases': []}
                    self.Battery[unit_mrid]['name'] = unit.name
                    self.Battery[unit_mrid]['ratedS'] = pec.ratedS
                    self.Battery[unit_mrid]['ratedE'] = unit.ratedE
                else:
                    self.Solar[unit_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [], 'measurementPhases': []}
                    self.Solar[unit_mrid]['name'] = pec.name
                    self.Solar[unit_mrid]['ratedS'] = pec.ratedS
            
            if not pec.PowerElectronicsConnectionPhases:
                if unit_mrid in self.Battery:
                    self.Battery[unit_mrid]['phases'] = 'ABC'
                if unit_mrid in self.Solar:
                    self.Solar[unit_mrid]['phases'] = 'ABC'
            else:
                phases = []
                for phase in pec.PowerElectronicsConnectionPhases:
                    phases.append(phase.phase.value)
                if unit_mrid in self.Battery:
                    self.Battery[unit_mrid]['phases'] = phases
                if unit_mrid in self.Solar:
                    self.Solar[unit_mrid]['phases'] = phases
            
            for measurement in pec.Measurements:
                if unit_mrid in self.Battery:
                    self.Battery[unit_mrid]['measurementType'].append(measurement.measurementType)
                    self.Battery[unit_mrid]['measurementmRID'].append(measurement.mRID)
                    if  measurement.phases.value is not None:
                        self.Battery[unit_mrid]['measurementPhases'].append(measurement.phases.value)
                if unit_mrid in self.Solar:
                    self.Solar[unit_mrid]['measurementType'].append(measurement.measurementType)
                    self.Solar[unit_mrid]['measurementmRID'].append(measurement.mRID)
                    if  measurement.phases.value is not None:
                        self.Solar[unit_mrid]['measurementPhases'].append(measurement.phases.value)

        for ld in network.graph[EnergyConsumer].values():
            ld_mrid = ld.mRID
            self.EnergyConsumer[ld_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [], 'measurementPhases': []}
            self.EnergyConsumer[ld_mrid]['name'] = ld.name
            if not ld.EnergyConsumerPhase:
                self.EnergyConsumer[ld_mrid]['phases'] = 'ABC'
            else:
                phases = []
                for phase in ld.EnergyConsumerPhase:
                    phases.append(phase.phase.value)
                self.EnergyConsumer[ld_mrid]['phases'] = phases
            for measurement in ld.Measurements:
                self.EnergyConsumer[ld_mrid]['measurementType'].append(measurement.measurementType)
                self.EnergyConsumer[ld_mrid]['measurementmRID'].append(measurement.mRID)
                if  measurement.phases.value is not None:
                    self.EnergyConsumer[ld_mrid]['measurementPhases'].append(measurement.phases.value)

        simulation.add_onmeasurement_callback(self.on_measurement)
        # simulation.start_simulation()
    
    def find_phase(self, degree):
        ref_points = [0, 120, -120]
        closest = min(ref_points, key=lambda x: abs(degree - x))
        phases = {0: 'A', -120: 'B', 120: 'C'}
        return phases[closest]
    
    def pol2cart(self, mag, angle_deg):
        # Convert degrees to radians. GridAPPS-D spits angle in degrees
        angle_rad =  math.radians(angle_deg)
        p = mag * math.cos(angle_rad)
        q = mag * math.sin(angle_rad)
        return p, q
    
    def find_injection(self, object, measurements):

        for item in object:
            object[item]['P_inj'] = [0, 0, 0]
            object[item]['Q_inj'] = [0, 0, 0]
            meas_type = object[item]['measurementType']
            va_idx = [i for i in range(len(meas_type)) if meas_type[i] == 'VA']
            pnv_idx = [i for i in range(len(meas_type)) if meas_type[i] == 'PNV']
            soc_idx = [i for i in range(len(meas_type)) if meas_type[i] == 'SoC']
            if soc_idx:
                soc = measurements[object[item]['measurementmRID'][soc_idx[0]]]['value']
                self.Battery[item]['soc'] =  soc
            if 's' in object[item]['phases'][0]:
                angle = measurements[object[item]['measurementmRID'][pnv_idx[0]]]['angle']
                rho = measurements[object[item]['measurementmRID'][va_idx[0]]]['magnitude']
                phi = measurements[object[item]['measurementmRID'][va_idx[0]]]['angle']
                p, q = self.pol2cart(rho, phi)
                if self.find_phase(angle) == 'A':
                    object[item]['P_inj'][0] = 2 * p
                    object[item]['Q_inj'][0] = 2 * q
                    object[item]['phases'][0] = 'A'
                elif self.find_phase(angle) == 'B':
                    object[item]['P_inj'][1] = 2 * p
                    object[item]['Q_inj'][1] = 2 * q
                    object[item]['phases'][0] = 'B'
                else:
                    object[item]['P_inj'][2] = 2 * p
                    object[item]['Q_inj'][2] = 2 * q
                    object[item]['phases'][0] = 'C'
            elif object[item]['phases'] == 'ABC':
                for k in range(3):
                    rho = measurements[object[item]['measurementmRID'][va_idx[k]]]['magnitude']
                    phi = measurements[object[item]['measurementmRID'][va_idx[k]]]['angle']
                    p, q = self.pol2cart(rho, phi)
                    object[item]['P_inj'][k] = p
                    object[item]['Q_inj'][k] = q
            else:
                rho = measurements[object[item]['measurementmRID'][va_idx[0]]]['magnitude']
                phi = measurements[object[item]['measurementmRID'][va_idx[0]]]['angle']
                p, q = self.pol2cart(rho, phi)
                if object[item]['phases'][0] == 'A':
                    object[item]['P_inj'][0] = p
                    object[item]['Q_inj'][0] = q
                elif object[item]['phases'][0] == 'B':
                    object[item]['P_inj'][1] = p
                    object[item]['Q_inj'][1] = q
                else:
                    object[item]['P_inj'][2] = p
                    object[item]['Q_inj'][2] = q
    
    def optimize_battery(self, timestamp):
        # Define optimization variables
        n_batt = len(self.Battery)
        p_flow_A = cp.Variable(integer=False, name='p_flow_A')
        p_flow_B = cp.Variable(integer=False, name='p_flow_B')
        p_flow_C = cp.Variable(integer=False, name='p_flow_C')
        p_flow_mod_A = cp.Variable(integer=False, name='p_flow_mod_A')
        p_flow_mod_B = cp.Variable(integer=False, name='p_flow_mod_B')
        p_flow_mod_C = cp.Variable(integer=False, name='p_flow_mod_C')
        p_batt = cp.Variable(n_batt, integer=False, name='p_batt')
        soc = cp.Variable(n_batt, integer=False, name='soc')
        lambda_c = cp.Variable(n_batt, boolean=True, name='lambda_c')
        lambda_d = cp.Variable(n_batt, boolean=True, name='lambda_d')
        P_batt_A = cp.Variable(n_batt, integer=False, name='P_batt_A')
        P_batt_B = cp.Variable(n_batt, integer=False, name='P_batt_B')
        P_batt_C = cp.Variable(n_batt, integer=False, name='P_batt_C')
        deltaT = 0.25

        constraints = []
        sum_flow_A, sum_flow_B, sum_flow_C = 0.0, 0.0, 0.0
        for pv in self.Solar:
            sum_flow_A += self.Solar[pv]['P_inj'][0]
            sum_flow_B += self.Solar[pv]['P_inj'][1]
            sum_flow_C += self.Solar[pv]['P_inj'][2]
        for load in self.EnergyConsumer:
            sum_flow_A += self.EnergyConsumer[load]['P_inj'][0]
            sum_flow_B += self.EnergyConsumer[load]['P_inj'][1]
            sum_flow_C += self.EnergyConsumer[load]['P_inj'][2]
        print(f'Substation flow without optimization at {timestamp}: ', sum_flow_A, sum_flow_B, sum_flow_C)
        idx = 0
        # For now, we assume battery to be 100% efficient. Need to rewrite the soc constraints if using different efficiency
        for batt in self.Battery:
            constraints.append(soc[idx] == self.Battery[batt]['soc'] / 100 + p_batt[idx] * deltaT / self.Battery[batt]['ratedE'])
            constraints.append(p_batt[idx] <= lambda_c[idx] * self.Battery[batt]['ratedS'])
            constraints.append(p_batt[idx] >= - lambda_d[idx] * self.Battery[batt]['ratedS'])
            constraints.append(lambda_c[idx] + lambda_d[idx] <= 1)
            constraints.append(soc[idx] <= 0.9)
            constraints.append(soc[idx] >= 0.2)
            # Tracking dispatch in A, B, and C phase by different batteries
            if 'ABC' in self.Battery[batt]['phases']:
                constraints.append(P_batt_A[idx] == p_batt[idx] / 3)
                constraints.append(P_batt_B[idx] == p_batt[idx] / 3)
                constraints.append(P_batt_C[idx] == p_batt[idx] / 3)
            else:
                if 'A' in self.Battery[batt]['phases']:
                    constraints.append(P_batt_A[idx] == p_batt[idx])
                    constraints.append(P_batt_B[idx] == 0.0)
                    constraints.append(P_batt_C[idx] == 0.0)
                if 'B' in self.Battery[batt]['phases']:
                    constraints.append(P_batt_A[idx] == 0.0)
                    constraints.append(P_batt_B[idx] == p_batt[idx])
                    constraints.append(P_batt_C[idx] == 0.0)
                if 'C' in self.Battery[batt]['phases']:
                    constraints.append(P_batt_A[idx] == 0.0)
                    constraints.append(P_batt_B[idx] == 0.0)
                    constraints.append(P_batt_C[idx] == p_batt[idx])
            idx += 1
        
        # Constraints for flow in phase ABC
        constraints.append(p_flow_A == sum_flow_A + sum(P_batt_A[k] for k in range(n_batt)))
        constraints.append(p_flow_B == sum_flow_B + sum(P_batt_B[k] for k in range(n_batt)))
        constraints.append(p_flow_C == sum_flow_C + sum(P_batt_C[k] for k in range(n_batt)))

        # Individual modulus is needed to make sure one phase doesn't compensate for other
        constraints.append(p_flow_mod_A >= p_flow_A )
        constraints.append(p_flow_mod_A >= - p_flow_A)

        constraints.append(p_flow_mod_B >= p_flow_B)
        constraints.append(p_flow_mod_B >= - p_flow_B )

        constraints.append(p_flow_mod_C >= p_flow_C)
        constraints.append(p_flow_mod_C >= - p_flow_C)

        # Objective function and invoke a solver
        objective = (p_flow_mod_A + p_flow_mod_B + p_flow_mod_C) 
        problem = cp.Problem(cp.Minimize(objective), constraints)
        problem.solve()

        print('Optimization status:', problem.status, flush=True)
        print('Substaton flow after optimization: ', p_flow_mod_A.value, p_flow_mod_B.value, p_flow_mod_C.value)
        print('Objective Function: ', problem.value)

        idx = 0
        dispatch_batteries = {}
        for batt in self.Battery:
            name = self.Battery[batt]['name']
            print(f'{name} dispatch and SOC: ', p_batt[idx].value, soc[idx].value,  P_batt_A[idx].value, P_batt_B[idx].value, P_batt_C[idx].value)
            dispatch_batteries[batt] = {}
            dispatch_batteries[batt]['p_batt'] =  p_batt[idx].value
            idx += 1
        
        return dispatch_batteries


    def on_measurement(self, sim: Simulation, timestamp: dict, measurements: dict) -> None:
        # print(timestamp)
        if self._count % 10 == 0:
            # Call function to extract simulation measurements from injection sources
            self.find_injection(self.Battery, measurements)
            self.find_injection(self.EnergyConsumer, measurements)
            self.find_injection(self.Solar, measurements)

            # Check injection from measurements
            for batt in self.Battery:
                print(batt, self.Battery[batt]['name'], self.Battery[batt]['P_inj'], self.Battery[batt]['soc'])
            print('........')

            # Invoke optimization for given grid condition
            dispatch_values = self.optimize_battery(timestamp)

            # Dispatch battery. Note that -ve values in forward difference means charging batteries
            # Make necessary changes in sign convention from optimization values            
            for unit in dispatch_values:
                self._init_batt_diff.add_difference(unit, 'PowerElectronicsConnection.p', - dispatch_values[unit]['p_batt'], 0.0)
                msg = self._init_batt_diff.get_message()
                self.gad_obj.send(self._publish_to_topic, json.dumps(msg))
        self._count += 1


def createSimulation(gad_obj: GridAPPSD, model_info: Dict[str, Any]) -> Simulation:
    # if not isinstance(gad_obj, GridAPPSD):
    #     raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
    # if not isinstance(model_info, dict):
    #     raise TypeError(f'The model_id must be a dictionary type.')
    line_name = model_info.get('modelId')
    subregion_name = model_info.get('subRegionId')
    region_name = model_info.get('regionId')
    sim_name = model_info.get('modelName')
    # if line_name is None:
    #     raise ValueError(f'Bad model info dictionary. The dictionary is missing key modelId.')
    # if subregion_name is None:
    #     raise ValueError(f'Bad model info dictionary. The dictionary is missing key subRegionId.')
    # if region_name is None:
    #     raise ValueError(f'Bad model info dictionary. The dictionary is missing key regionId.')
    # if sim_name is None:
    #     raise ValueError(f'Bad model info dictionary. The dictionary is missing key modelName.')
    # psc = PowerSystemConfig(Line_name=line_name,
    #                         SubGeographicalRegion_name=subregion_name,
    #                         GeographicalRegion_name=region_name)
    # start_time = int(datetime.utcnow().replace(microsecond=0).timestamp())
    # sim_args = SimulationArgs(
    #     start_time=f'{start_time}',
    # #   duration=f'{24*3600}',
    #     duration=120,
    #     simulation_name=sim_name,
    #     run_realtime=False
    #     # pause_after_measurements=True
    #     )
    # sim_config = SimulationConfig(power_system_config=psc, simulation_config=sim_args)
    # sim_obj = Simulation(gapps=gad_obj, run_config=sim_config)
    system_config = PowerSystemConfig(
            GeographicalRegion_name=region_name,
            SubGeographicalRegion_name=subregion_name,
            Line_name=line_name
        )

    model_config = ModelCreationConfig(
        load_scaling_factor=1,
        schedule_name="ieeezipload",
        z_fraction=0,
        i_fraction=1,
        p_fraction=0,
        randomize_zipload_fractions=False,
        use_houses=False
    )

    start = datetime(2024, 1, 1)
    epoch = datetime.timestamp(start)
    duration = timedelta(minutes=7).total_seconds()
    # using hadrcode epoch to ensure afternoon time
    epoch = 1704207000

    sim_args = SimulationArgs(
        start_time=epoch,
        duration=duration,
        simulator="GridLAB-D",
        timestep_frequency=1000,
        timestep_increment=1000,
        run_realtime=True,
        simulation_name=sim_name,
        power_flow_solver_method="NR",
        model_creation_config=asdict(model_config)
    )

    sim_config = SimulationConfig(
        power_system_config=asdict(system_config),
        simulation_config=asdict(sim_args)
    )

    sim_obj = Simulation(gad_obj, asdict(sim_config))
    return sim_obj


def main(control_enabled: bool, start_simulations: bool, model_id: str = None):
    if not isinstance(control_enabled, bool):
        raise TypeError(f'Argument, control_enabled, must be a boolean.')
    if not isinstance(start_simulations, bool):
        raise TypeError(f'Argument, start_simulations, must be a boolean.')
    if not isinstance(model_id, str) and model_id is not None:
        raise TypeError(
            f'The model id passed to the convervation voltage reduction application must be a string type or {None}.')
    
    gapps = GridAPPSD(username='system', password='manager')
    cim_profile = 'rc4_2021'
    cim = importlib.import_module('cimgraph.data_profile.' + cim_profile)
    feeder = cim.Feeder(mRID=model_id)
    params = ConnectionParameters(
        url="http://localhost:8889/bigdata/namespace/kb/sparql",
        cim_profile=cim_profile)

    gapps = GridAPPSD(username='system', password='manager')

    bg = BlazegraphConnection(params)
    network = FeederModel(
        connection=bg,
        container=feeder,
        distributed=False)
    utils.get_all_data(network)


    local_simulations = {}
    app_instances = {'field_instances': {}, 'external_simulation_instances': {}, 'local_simulation_instances': {}}
    response = gapps.query_model_info()
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
            local_simulations[m.get('modelId', '')] = createSimulation(gapps, m)
    else:
        #TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
        pass
    # Create an cvr controller instance for all the real systems in the database
    # for m in models:
    #     m_id = m.get('modelId')
    #     app_instances['field_instances'][m_id] = ConservationVoltageReductionController(gad_object, m_id)
    
    for m_id, simulation in local_simulations.items():
        measurements_topic = topics.simulation_output_topic(simulation)
        simulation.start_simulation()
        c_mapp = CarbonManagementApp(
            gapps, m_id, network, simulation=simulation)
        # gapps.subscribe(measurements_topic, c_mapp)
        while True:
            time.sleep(0.1)



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('model_id', nargs='?', default=None, help='The unit_mrid of the cim model to perform cvr on.')
    parser.add_argument('-s',
                        '--start_simulations',
                        action='store_true',
                        help='Flag to have application start simulations')
    parser.add_argument('-d',
                        '--disable_control',
                        action='store_true',
                        help='Flag to disable control on startup by default.')
    args = parser.parse_args()
    args.start_simulations = True
    args.model_id = '_EE71F6C9-56F0-4167-A14E-7F4C71F10EAA'
    main(args.disable_control, args.start_simulations, args.model_id)