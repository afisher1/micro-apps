from argparse import ArgumentParser
from cmath import exp
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

import csv
import importlib
import json
import math
import os
import time

from cimgraph import utils
from cimgraph.databases import ConnectionParameters, BlazegraphConnection
from cimgraph.models import FeederModel
from gridappsd import GridAPPSD, topics, DifferenceBuilder
from gridappsd.simulation import ModelCreationConfig, PowerSystemConfig, Simulation, SimulationArgs, SimulationConfig
from tabulate import tabulate

import cimgraph.data_profile.cimhub_2023 as cim
import cvxpy as cp


# IEEE123_APPS = "E3D03A27-B988-4D79-BFAB-F9D37FB289F7"
IEEE13 = "49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"
IEEE123 = "C1C3E687-6FFD-C753-582B-632A27E28507"
IEEE123PV = "A3BC35AA-01F6-478E-A7B1-8EA4598A685C"
IEEE123_APPS = "F49D1288-9EC6-47DB-8769-57E2B6EDB124"
ieee123_apps_feeder_head_measurement_mrids = [
    "43c1d677-586e-4ef5-b35c-f08d326af31c",
    "cd3e1fca-6826-4d8b-8d42-5cb5817e085f",
    "36e2d53e-6e91-4a0c-a1ce-a196ff422332"
]
SOURCE_BUS = '150r'
OUT_DIR = "outputs"
ROOT = os.getcwd()


class CarbonManagementApp(object):
    """Carbon Management Class
        This class implements a centralized algorithm for carbon management in a grid by controlling batteries in the distribution model.

        Attributes:
            sim_id: The simulation id that the instance will be perfoming control on. If the sim_id is None then the
                    application will assume it is performing control on the actual field devices.
                Type: str.
                Default: None.
            gad_obj: An instatiated object that is connected to the gridappsd message bus usually this should be the same object which subscribes, but that
                    isn't required.
                Type: GridAPPSD.
                Default: None.
            model_id: The mrid of the cim model to perform carbon management control on.
                Type: str.
                Default: None.
            simulation: Simulation object of the identified cim model.
                Type: Simulation.
                Default: None.
            network: An instatiated object of knowledge graph class for distribution feeder objects.
                Type: FeederModel.
                Default: None.
        Methods:
            initialize(headers:Dict[Any], message:Dict[Any]): This callback function will trigger the application to
                    read the model database to find all the measurements in the model as well as batteries in the sytem.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            on_measurement(headers:Dict[Any], message:Dict[Any]): This callback function will be used to capture the
              measurements dictionary needed for control.
                Arguments:
                    headers: A dictionary containing header information on the message received from the GridAPPS-D
                            platform.
                        Type: dictionary.
                        Default: NA.
                    message: A dictionary containing the message received from the GridAPPS-D platform.
                        Type: dictionary.
                        Default: NA.
                Returns: NA.
            optimize_battery(): This is the main function for controlling the battery.
    """

    def __init__(self,
                 gad_obj: GridAPPSD,
                 model_id: str,
                 network: FeederModel,
                 sim_id: str = None,
                 simulation: Simulation = None):
        if not isinstance(gad_obj, GridAPPSD):
            raise TypeError(f'gad_obj must be an instance of GridAPPSD!')
        if not isinstance(model_id, str):
            raise TypeError(f'model_id must be a str type.')
        if model_id is None or model_id == '':
            raise ValueError(f'model_id must be a valid uuid.')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'sim_id must be a string type or {None}!')
        if not isinstance(simulation, Simulation) and simulation is not None:
            raise TypeError(f'The simulation arg must be a Simulation type or {None}!')
        self.simulation = simulation
        self.gad_obj = gad_obj
        self.init_batt_dis = True
        self._count = 0
        self._publish_to_topic = topics.simulation_input_topic(simulation.simulation_id)
        self._init_batt_diff = DifferenceBuilder(simulation.simulation_id)

        self.Battery = {}
        self.Solar = {}
        self.EnergyConsumer = {}
        self.peak_va_measurements_A = {}
        self.peak_va_measurements_B = {}
        self.peak_va_measurements_C = {}
        self.has_batteries = True
        self.has_power_electronics = True
        self.has_energy_consumers = True
        if cim.PowerElectronicsConnection not in network.graph:
            self.has_power_electronics = False
            self.has_batteries = False
            # raise ValueError("No power electronic devices in network.")
        elif len(network.graph[cim.PowerElectronicsConnection].keys()) == 0:
            self.has_power_electronics = False
            self.has_batteries = False
        if cim.EnergyConsumer not in network.graph:
            self.has_energy_consumers = False

        if self.has_power_electronics:
            self._collect_power_electronic_devices(network)
            # raise ValueError("No power electronic devices in network.")

        if len(self.Battery) == 0:
            self.has_batteries = False
            print("No batteries in system.")
            # raise ValueError("No batteries in network.")

        self.network = network
        self.findFeederHeadLoadMeasurements()

        if self.has_energy_consumers:
            self._collect_energy_consumers(network)
        simulation.add_onmeasurement_callback(self.on_measurement)
        # data output
        out_dir = Path(__file__).parent/"output"
        feeder = network.graph.get(cim.Feeder, {}).get(model_id)
        self.model_results_path = out_dir/feeder.name
        self.model_results_path.mkdir(parents=True, exist_ok=True)
        # self.findFeederHeadLoadMeasurements()
        self.init_output_file()

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
        with open(self.model_results_path / "optimization_summary.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "load_pv_a", "load_pv_b", "load_pv_c", "load_pv_batt_a", "load_pv_batt_b", "load_pv_batt_c", "status"]
            writer.writerow(header)
        with open(self.model_results_path / "optimization_result.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "battery", "phases", "p_batt"]
            writer.writerow(header)
        with open(self.model_results_path / "simulation_table.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header=["time","mrid", "battery","phases","p_a","p_b","p_c","soc"]
            writer.writerow(header)
        with open(self.model_results_path / "dispatches.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "value"]
            writer.writerow(header)
        with open(self.model_results_path / "feeder_head.csv", mode='a', newline='') as file:
            writer = csv.writer(file)
            header = ["time", "mrid", "phase", "p", "q"]
            writer.writerow(header)

    def get_feeder_head_measurements(self, measurements):
        total_loads = {}
        if self.network.container.mRID == IEEE123_APPS:
            for mrid in ieee123_apps_feeder_head_measurement_mrids:
                phase = self.network.graph.get(cim.Analog, {}).get(mrid).phases.value
                s_magnitude = measurements.get(mrid).get("magnitude")
                s_angle_rad = measurements.get(mrid).get("angle")
                total_loads[phase] = s_magnitude*exp(1j*s_angle_rad)
        return total_loads

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

    def _collect_power_electronic_devices(self, network):
        for pec in network.graph.get(cim.PowerElectronicsConnection, {}).values():
            # inv_mrid = pec.mRID
            for unit in pec.PowerElectronicsUnit:
                unit_mrid = unit.mRID
                if isinstance(unit, cim.BatteryUnit):
                    self.Battery[unit_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [],
                                               'measurementPhases': []}
                    self.Battery[unit_mrid]['name'] = unit.name
                    self.Battery[unit_mrid]['ratedS'] = float(pec.ratedS) / 1000
                    self.Battery[unit_mrid]['ratedE'] = float(unit.ratedE) / 1000
                else:
                    self.Solar[unit_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [],
                                             'measurementPhases': []}
                    self.Solar[unit_mrid]['name'] = pec.name
                    self.Solar[unit_mrid]['ratedS'] = float(pec.ratedS) / 1000

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
                    if measurement.phases.value is not None:
                        self.Battery[unit_mrid]['measurementPhases'].append(measurement.phases.value)
                if unit_mrid in self.Solar:
                    self.Solar[unit_mrid]['measurementType'].append(measurement.measurementType)
                    self.Solar[unit_mrid]['measurementmRID'].append(measurement.mRID)
                    if measurement.phases.value is not None:
                        self.Solar[unit_mrid]['measurementPhases'].append(measurement.phases.value)

    def _collect_energy_consumers(self, network):
        for ld in network.graph.get(cim.EnergyConsumer, {}).values():
            ld_mrid = ld.mRID
            self.EnergyConsumer[ld_mrid] = {'phases': [], 'measurementType': [], 'measurementmRID': [],
                                            'measurementPhases': []}
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
                if measurement.phases.value is not None:
                    self.EnergyConsumer[ld_mrid]['measurementPhases'].append(measurement.phases.value)

    def find_phase(self, degree):
        ref_points = [0, 120, -120]
        closest = min(ref_points, key=lambda x: abs(degree - x))
        phases = {0: 'A', -120: 'B', 120: 'C'}
        return phases[closest]

    def pol2cart(self, mag, angle_deg):
        # Convert degrees to radians. GridAPPS-D spits angle in degrees
        angle_rad = math.radians(angle_deg)
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
                self.Battery[item]['soc'] = soc
            if 's' in object[item]['phases'][0]:
                angle = measurements[object[item]['measurementmRID'][pnv_idx[0]]]['angle']
                rho = measurements[object[item]['measurementmRID'][va_idx[0]]]['magnitude']
                phi = measurements[object[item]['measurementmRID'][va_idx[0]]]['angle']
                p, q = self.pol2cart(rho, phi)
                if self.find_phase(angle) == 'A':
                    object[item]['P_inj'][0] = 2 * p / 1000
                    object[item]['Q_inj'][0] = 2 * q / 1000
                    object[item]['phases'] = 'A'
                elif self.find_phase(angle) == 'B':
                    object[item]['P_inj'][1] = 2 * p / 1000
                    object[item]['Q_inj'][1] = 2 * q / 1000
                    object[item]['phases'] = 'B'
                else:
                    object[item]['P_inj'][2] = 2 * p / 1000
                    object[item]['Q_inj'][2] = 2 * q / 1000
                    object[item]['phases'] = 'C'
            elif object[item]['phases'] == 'ABC':
                for k in range(3):
                    rho = measurements[object[item]['measurementmRID'][va_idx[k]]]['magnitude']
                    phi = measurements[object[item]['measurementmRID'][va_idx[k]]]['angle']
                    p, q = self.pol2cart(rho, phi)
                    object[item]['P_inj'][k] = p / 1000
                    object[item]['Q_inj'][k] = q / 1000
            else:
                rho = measurements[object[item]['measurementmRID'][va_idx[0]]]['magnitude']
                phi = measurements[object[item]['measurementmRID'][va_idx[0]]]['angle']
                p, q = self.pol2cart(rho, phi)
                if object[item]['phases'][0] == 'A':
                    object[item]['P_inj'][0] = p / 1000
                    object[item]['Q_inj'][0] = q / 1000
                elif object[item]['phases'][0] == 'B':
                    object[item]['P_inj'][1] = p / 1000
                    object[item]['Q_inj'][1] = q / 1000
                else:
                    object[item]['P_inj'][2] = p / 1000
                    object[item]['Q_inj'][2] = q / 1000

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
        n_batt_ABC = 0
        for batt in self.Battery:
            if 'ABC' in self.Battery[batt]['phases']:
                n_batt_ABC += 1
        # Defining variable to constraint same sign for multiple 3-ph batteries
        if n_batt_ABC > 0:
            b = cp.Variable(n_batt_ABC, boolean=True, name='b')

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

        idx = 0
        idx_b = 0
        # For now, we assume battery to be 100% efficient. Need to rewrite the soc constraints if using different efficiency
        for batt in self.Battery:
            constraints.append(
                soc[idx] == self.Battery[batt]['soc'] / 100 + p_batt[idx] * deltaT / self.Battery[batt]['ratedE'])
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
                constraints.append(p_batt[idx] >= - b[idx_b] * self.Battery[batt]['ratedS'])
                constraints.append(p_batt[idx] <= self.Battery[batt]['ratedS'] * (1 - b[idx_b]))
                idx_b += 1
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
        # Ensuring three phase batteries have the same sign. We don't want one charging and another discharging
        # Although, mathematically it might sound correct, it is not worth to do such dispatch.
        for k in range(n_batt_ABC - 1):
            constraints.append(b[k] == b[k + 1])

        # Constraints for flow in phase ABC
        constraints.append(p_flow_A == sum_flow_A + sum(P_batt_A[k] for k in range(n_batt)))
        constraints.append(p_flow_B == sum_flow_B + sum(P_batt_B[k] for k in range(n_batt)))
        constraints.append(p_flow_C == sum_flow_C + sum(P_batt_C[k] for k in range(n_batt)))

        # Individual modulus is needed to make sure one phase doesn't compensate for other
        constraints.append(p_flow_mod_A >= p_flow_A)
        constraints.append(p_flow_mod_A >= - p_flow_A)

        constraints.append(p_flow_mod_B >= p_flow_B)
        constraints.append(p_flow_mod_B >= - p_flow_B)

        constraints.append(p_flow_mod_C >= p_flow_C)
        constraints.append(p_flow_mod_C >= - p_flow_C)

        # Objective function and invoke a solver
        objective = (p_flow_mod_A + p_flow_mod_B + p_flow_mod_C)
        problem = cp.Problem(cp.Minimize(objective), constraints)
        problem.solve()

        # Extract optimization solution
        idx = 0
        dispatch_batteries = {}
        optimization_solution_table = []
        for batt in self.Battery:
            name = self.Battery[batt]['name']
            dispatch_batteries[batt] = {}
            if p_batt[idx].value is None:
                continue
            dispatch_batteries[batt]['p_batt'] = p_batt[idx].value * 1000
            optimization_solution_table.append([name, self.Battery[batt]['phases'], p_batt[idx].value])
            data = [timestamp, batt, name, self.Battery[batt]['phases'], p_batt[idx].value]
            header = ["time", "battery", "phases", "p_batt"]
            add_data_to_csv(self.model_results_path/"optimization_result.csv", data, header=header)
            idx += 1
        print('Optimization Solution')
        print(tabulate(optimization_solution_table, headers=['Battery', 'phases', 'P_batt (kW)'], tablefmt='psql'))
        if problem.status == "optimal":
            load_pv = ['{:.3f}'.format(sum_flow_A), '{:.3f}'.format(sum_flow_B), '{:.3f}'.format(sum_flow_C)]
            load_pv_batt = ['{:.3f}'.format(p_flow_A.value), '{:.3f}'.format(p_flow_B.value),
                            '{:.3f}'.format(p_flow_C.value)]
            optimization_summary = []
            optimization_summary.append([load_pv, load_pv_batt, problem.status])
            data = [timestamp]
            data.extend(load_pv)
            data.extend(load_pv_batt)
            data.append(problem.status)
            header = ["time", "load_pv_a", "load_pv_b", "load_pv_c", "load_pv_batt_a", "load_pv_batt_b", "load_pv_batt_c", "status"]
            add_data_to_csv(self.model_results_path/"optimization_summary.csv", data, header=header)
            print(tabulate(optimization_summary, headers=['Load+PV (kW)', 'Load+PV+Batt (kW)', 'Status'], tablefmt='psql'))
        return dispatch_batteries

    def findFeederHeadLoadMeasurements(self):
        energySources = self.network.graph.get(cim.EnergySource, {})
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

    def on_measurement(self, sim: Simulation, timestamp: dict, measurements: dict) -> None:
        if self.simulation is not None:
            self.simulation.pause()
        self.timestamp = timestamp
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
        if not self.has_batteries:
            return
        if self._count % 10 == 0:
            # Call function to extract simulation measurements from injection sources
            self.find_injection(self.Battery, measurements)
            self.find_injection(self.EnergyConsumer, measurements)
            self.find_injection(self.Solar, measurements)

            # Display real-time battery injection and current SOC from measurements
            simulation_table_batteries = []
            for batt in self.Battery:
                name = self.Battery[batt]['name']
                phases = self.Battery[batt]['phases']
                data = [timestamp, batt, name, phases]
                simulation_table_batteries.append(
                    [name, phases, self.Battery[batt]['P_inj'], self.Battery[batt]['soc']])
                data.extend(self.Battery[batt]['P_inj'])
                data.extend([self.Battery[batt]['soc']])
                header=["time","mrid", "battery","phases","p_a","p_b","p_c","soc"]
                add_data_to_csv(self.model_results_path/"simulation_table.csv", data, header=header)
            print(f'\n.......Curren timestamp: {timestamp}.......\n')
            print('Simulation Table')
            print(tabulate(simulation_table_batteries, headers=['Battery', 'phases', 'P_batt (kW)', 'SOC'],
                           tablefmt='psql'))

            # Invoke optimization for given grid condition
            dispatch_values = self.optimize_battery(timestamp)

            # Dispatch battery. Note that -ve values in forward difference means charging batteries
            # Make necessary changes in sign convention from optimization values
            for unit in dispatch_values:
                self._init_batt_diff.add_difference(unit, 'PowerElectronicsConnection.p',
                                                    - dispatch_values[unit]['p_batt'], 0.0)
                msg = self._init_batt_diff.get_message()
                with open(self.model_results_path/f"dispatch_{timestamp}.json", "w") as f:
                    json.dump(msg, f, indent=4)

                header = ["time", "mrid", "value"]
                data = [timestamp, unit, -dispatch_values[unit]['p_batt']]
                add_data_to_csv(self.model_results_path/"dispatches.csv", data)
                self.gad_obj.send(self._publish_to_topic, json.dumps(msg))

        self._count += 1
        if self.simulation is not None:
            self.simulation.resume()


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

    start = datetime(2023, 1, 1, 0, tzinfo=timezone.utc)
    epoch = datetime.timestamp(start)
    duration = timedelta(hours=24).total_seconds()
    # using hadrcode epoch to ensure afternoon time
    # epoch = 1704207000

    sim_args = SimulationArgs(
        start_time=epoch,
        duration=duration,
        simulator="GridLAB-D",
        timestep_frequency=1000,
        timestep_increment=1000,
        run_realtime=False,
        simulation_name=sim_name,
        power_flow_solver_method="NR",
        model_creation_config=asdict(model_config),
        pause_after_measurements=True,
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
        # TODO: query platform for running simulations which is currently not implemented in the GridAPPS-D Api
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
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Simulation manually stopped.")
        finally:
            print(" -- Stopping simulation -- ")
            simulation.stop()


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
    # args.start_simulations = True
    # args.model_id = '_EE71F6C9-56F0-4167-A14E-7F4C71F10EAA'
    main(args.disable_control, args.start_simulations, args.model_id)
