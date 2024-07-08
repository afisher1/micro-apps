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
        self.peak_va_measurements_A = {}
        self.peak_va_measurements_B = {}
        self.peak_va_measurements_C = {}
        self.peak_setpoint_A = None
        self.peak_setpoint_B = None
        self.peak_setpoint_C = None
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
        # Read model_id from cimgraph to get all the controllable batteries, and measurements.
        self.graph_model = buildGraphModel(model_id)
        self.installed_battery_capacity_A = 0.0
        self.installed_battery_capacity_B = 0.0
        self.installed_battery_capacity_C = 0.0
        self.installed_battery_power_A = 0.0
        self.installed_battery_power_B = 0.0
        self.installed_battery_power_C = 0.0
        self.configureBatteryProperties()
        # print(measurements.keys())
        if peak_setpoint is not None:
            self.peak_setpoint_A = peak_setpoint / 3.0
            self.peak_setpoint_B = peak_setpoint / 3.0
            self.peak_setpoint_C = peak_setpoint / 3.0
        else:
            self.configurePeakShavingSetpoint()
        self.findFeederHeadLoadMeasurements()
        if self.simulation is not None:
            self.simulation.start_simulation()

        self.isValid = True
        self.first_message = True

    def configureBatteryProperties(self):
        '''This function uses cimgraph to populate the following properties:

            self.controllable_batteries_A
            self.controllable_batteries_B
            self.controllable_batteries_C
        '''
        powerElectronicsConnections = self.graph_model.graph.get(cim.PowerElectronicsConnection, {})
        for pec in powerElectronicsConnections:
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
                    self.controllable_batteries_A[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.B, cim.PhaseCode.BN]:
                    self.installed_battery_capacity_B += batteryCapacity
                    self.installed_battery_power_B += float(pec.ratedS)
                    self.controllable_batteries_B[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.C, cim.PhaseCode.CN]:
                    self.installed_battery_capacity_C += batteryCapacity
                    self.installed_battery_power_C += float(pec.ratedS)
                    self.controllable_batteries_C[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.AB, cim.PhaseCode.ABN]:
                    self.installed_battery_capacity_A += batteryCapacity / 2.0
                    self.installed_battery_power_A += float(pec.ratedS) / 2.0
                    self.controllable_batteries_A[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        }
                    }
                    self.installed_battery_capacity_B += batteryCapacity / 2.0
                    self.installed_battery_power_B += float(pec.ratedS) / 2.0
                    self.controllable_batteries_B[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.AC, cim.PhaseCode.ACN]:
                    self.installed_battery_capacity_A += batteryCapacity / 2.0
                    self.installed_battery_power_A += float(pec.ratedS) / 2.0
                    self.controllable_batteries_A[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        }
                    }
                    self.installed_battery_capacity_C += batteryCapacity / 2.0
                    self.installed_battery_power_C += float(pec.ratedS) / 2.0
                    self.controllable_batteries_C[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.BC, cim.PhaseCode.BCN]:
                    self.installed_battery_capacity_B += batteryCapacity / 2.0
                    self.installed_battery_power_B += float(pec.ratedS) / 2.0
                    self.controllable_batteries_B[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        }
                    }
                    self.installed_battery_capacity_C += batteryCapacity / 2.0
                    self.installed_battery_power_C += float(pec.ratedS) / 2.0
                    self.controllable_batteries_C[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        }
                    }
                elif inverterPhases in [cim.PhaseCode.ABC, cim.PhaseCode.ABCN]:
                    self.installed_battery_capacity_A += batteryCapacity / 3.0
                    self.installed_battery_power_A += float(pec.ratedS) / 3.0
                    self.controllable_batteries_A[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.A, cim.PhaseCode.AN]),
                            'value': None
                        }
                    }
                    self.installed_battery_capacity_B += batteryCapacity / 3.0
                    self.installed_battery_power_B += float(pec.ratedS) / 3.0
                    self.controllable_batteries_B[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.B, cim.PhaseCode.BN]),
                            'value': None
                        }
                    }
                    self.installed_battery_capacity_C += batteryCapacity / 3.0
                    self.installed_battery_power_C += float(pec.ratedS) / 3.0
                    self.controllable_batteries_C[pec.mrid] = {
                        'object': pec,
                        'maximum_power': float(pec.ratedS),
                        'power_measurement': {
                            'object': findMeasurement(pec.Measurements, 'VA', [cim.PhaseCode.C, cim.PhaseCode.CN]),
                            'value': None
                        },
                        'soc_measurement': {
                            'object': findMeasurement(pec.Measurements, 'SoC', [cim.PhaseCode.C, cim.PhaseCode.CN]),
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
        for pecp in cimObj.PowerElectronicsConnectionPhases:
            if pecp.phase != cim.SinglePhaseKind.s1 and pecp.phase != cim.SinglePhaseKind.s2:
                strPhases += pecp.phase.value
            else:
                strPhases = pecp.phase.value
        if 'A' in strPhases or 'B' in strPhases or 'C' in strPhases:
            phaseCodeStr = ''
            if 'A' in strPhases:
                phaseCodeStr += 'A'
            if 'B' in strPhases:
                phaseCodeStr += 'B'
            if 'C' in strPhases:
                phaseCodeStr += 'C'
            phaseCode = cim.PhaseCode('phaseCodeStr')
        else:    # inverter is on the secondary system need to trace up to the centertapped tansformer.
            phaseCode = self.findPrimaryPhase(cimObj)
        return phaseCode

    def configurePeakShavingSetpoint(self):
        energySources = self.graph_model.graph.get(cim.EnergySource, {})
        feederPowerRating = 0.0
        for source in energySources.values:
            feederPowerRating = findFeederPowerRating(source)
        self.peak_setpoint_A = (feederPowerRating / 3.0) - (self.installed_battery_capacity_A * 0.95)
        self.peak_setpoint_B = (feederPowerRating / 3.0) - (self.installed_battery_capacity_B * 0.95)
        self.peak_setpoint_C = (feederPowerRating / 3.0) - (self.installed_battery_capacity_C * 0.95)

    def findFeederHeadLoadMeasurements(self):
        energySources = self.graph_model.graph.get(cim.EnergySource, {})
        feederTransformer = None
        for source in energySources.values:
            feederTransformer = findFeederTransformer(source)
            transformerMeasurements = feederTransformer.Measurements
            for measurement in transformerMeasurements:
                if measurement.measurementType == 'VA' and int(measurement.Terminal.sequenceNumber) == 1:
                    if measurement.phases in [cim.PhaseCode.A, cim.PhaseCode.AN]:
                        self.peak_va_measurements_A[measurement.mRID] = {'object': measurement, 'value': None}
                    elif measurement.phases in [cim.PhaseCode.B, cim.PhaseCode.BN]:
                        self.peak_va_measurements_B[measurement.mRID] = {'object': measurement, 'value': None}
                    elif measurement.phases in [cim.PhaseCode.C, cim.PhaseCode.CN]:
                        self.peak_va_measurements_C[measurement.mRID] = {'object': measurement, 'value': None}
        if not self.peak_va_measurements_A or self.peak_va_measurements_B or self.peak_va_measurements_C:
            raise RuntimeError(f'feeder {self.graph_model.container.mRID}, has no measurements associated with the '
                               'feeder head transformer!')

    def on_measurement(self, sim: Simulation, timestamp: str, measurements: Dict[str, Dict]):
        self.desired_setpoints.clear()
        #TODO: update measurements
        for mrid in self.peak_va_measurements_A.keys():
            measurement = measurements.get(self.peak_va_measurements_A[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_A[mrid]['value'] = measurement
        for mrid in self.peak_va_measurements_B.keys():
            measurement = measurements.get(self.peak_va_measurements_B[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_B[mrid]['value'] = measurement
        for mrid in self.peak_va_measurements_C.keys():
            measurement = measurements.get(self.peak_va_measurements_C[mrid]['object'].mRID)
            if measurement is not None:
                self.peak_va_measurements_C[mrid]['value'] = measurement
        for mrid in self.controllable_batteries_A.keys():
            measurement = measurements.get(self.controllable_batteries_A[mrid]['power_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_A[mrid]['power_measurement']['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_A[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_A[mrid]['soc_measurement']['value'] = measurement
        for mrid in self.controllable_batteries_B.keys():
            measurement = measurements.get(self.controllable_batteries_B[mrid]['power_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_B[mrid]['power_measurement']['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_B[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_B[mrid]['soc_measurement']['value'] = measurement
        for mrid in self.controllable_batteries_C.keys():
            measurement = measurements.get(self.controllable_batteries_C[mrid]['power_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_C[mrid]['power_measurement']['value'] = measurement
            measurement = measurements.get(self.controllable_batteries_C[mrid]['soc_measurement']['object'].mRID)
            if measurement is not None:
                self.controllable_batteries_C[mrid]['soc_measurement']['value'] = measurement
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

    def get_load_minus_batteries(self):
        load_A = 0
        load_B = 0
        load_C = 0
        for xfmr_id in self.peak_va_measurements_A.keys():
            measurement = self.peak_va_measurements_A[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_A += mag * math.cos(math.radians(ang_in_deg))
        for batt_id in self.controllable_batteries_A.keys():
            measurement = self.controllable_batteries_A[batt_id]['power_measurement'].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_A -= mag * math.cos(math.radians(ang_in_deg))
        for xfmr_id in self.peak_va_measurements_B.keys():
            measurement = self.peak_va_measurements_B[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_B += mag * math.cos(math.radians(ang_in_deg))
        for batt_id in self.controllable_batteries_B.keys():
            measurement = self.controllable_batteries_B[batt_id]['power_measurement'].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_B -= mag * math.cos(math.radians(ang_in_deg))
        for xfmr_id in self.peak_va_measurements_C.keys():
            measurement = self.peak_va_measurements_C[xfmr_id].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_C += mag * math.cos(math.radians(ang_in_deg))
        for batt_id in self.controllable_batteries_C.keys():
            measurement = self.controllable_batteries_C[batt_id]['power_measurement'].get('value')
            if measurement is not None:
                mag = measurement.get('magnitude')
                ang_in_deg = measurement.get('angle')
                if ((mag is not None) and (ang_in_deg is not None)):
                    load_C -= mag * math.cos(math.radians(ang_in_deg))
        return (load_A, load_B, load_C)

    def calc_batt_charge_A(self, power_to_charge_A, upper_limit):
        return_dict = {}
        available_capacity_A = self.installed_battery_power_A
        for batt_id in self.controllable_batteries_A.keys():
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_A -= self.controllable_batteries_A[batt_id]['maximum_power']
        for batt_id in self.controllable_batteries_A.keys():
            batt_soc = self.controllable_batteries_A[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    continue
            measurement = self.controllable_batteries_A[batt_id]['power_measurement'].get('value')
            if measurement is None:
                break
            mag = measurement.get('magnitude')
            ang_in_deg = measurement.get('angle')
            if (mag is None) or (ang_in_deg is None):
                break
            current_power = mag * math.cos(math.radians(ang_in_deg))
            new_power = (power_to_charge_A /
                         available_capacity_A) * self.controllable_batteries_A[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_A[batt_id]['maximum_power'])
            return_dict[batt_id] = {
                'object': self.controllable_batteries_A['object'],
                'old_setpoint': current_power,
                'setpoint': new_power
            }
        return return_dict

    def calc_batt_charge_B(self, power_to_charge_B, upper_limit):
        return_dict = {}
        available_capacity_B = self.installed_battery_power_B
        for batt_id in self.controllable_batteries_B.keys():
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_B -= self.controllable_batteries_B[batt_id]['maximum_power']
        for batt_id in self.controllable_batteries_B.keys():
            batt_soc = self.controllable_batteries_B[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    continue
            measurement = self.controllable_batteries_B[batt_id]['power_measurement'].get('value')
            if measurement is None:
                break
            mag = measurement.get('magnitude')
            ang_in_deg = measurement.get('angle')
            if (mag is None) or (ang_in_deg is None):
                break
            current_power = mag * math.cos(math.radians(ang_in_deg))
            new_power = (power_to_charge_B /
                         available_capacity_B) * self.controllable_batteries_B[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_B[batt_id]['maximum_power'])
            return_dict[batt_id] = {
                'object': self.controllable_batteries_B['object'],
                'old_setpoint': current_power,
                'setpoint': new_power
            }
        return return_dict

    def calc_batt_charge_C(self, power_to_charge_C, upper_limit):
        return_dict = {}
        available_capacity_C = self.installed_battery_power_C
        for batt_id in self.controllable_batteries_C.keys():
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    available_capacity_C -= self.controllable_batteries_C[batt_id]['maximum_power']
        for batt_id in self.controllable_batteries_C.keys():
            batt_soc = self.controllable_batteries_C[batt_id]['soc_measurement'].get('value')
            if batt_soc is not None:
                if batt_soc > upper_limit:
                    continue
            measurement = self.controllable_batteries_C[batt_id]['power_measurement'].get('value')
            if measurement is None:
                break
            mag = measurement.get('magnitude')
            ang_in_deg = measurement.get('angle')
            if (mag is None) or (ang_in_deg is None):
                break
            current_power = mag * math.cos(math.radians(ang_in_deg))
            new_power = (power_to_charge_C /
                         available_capacity_C) * self.controllable_batteries_C[batt_id]['maximum_power']
            new_power = min(new_power, self.controllable_batteries_C[batt_id]['maximum_power'])
            return_dict[batt_id] = {
                'object': self.controllable_batteries_C['object'],
                'old_setpoint': current_power,
                'setpoint': new_power
            }
        return return_dict

    def send_setpoints(self):
        self.differenceBuilder.clear()
        for mrid, dictVal in self.desired_setpoints.items():
            cimObj = dictVal.get('object')
            newSetpoint = dictVal.get('setpoint')
            oldSetpoint = dictVal.get('old_setpoint')
            if isinstance(cimObj, cim.PowerElectronicsConnection):
                self.differenceBuilder.add_difference(mrid, 'PowerElectronicsConnection.p', newSetpoint[0], oldSetpoint)
            else:
                self.log.warning(f'The CIM object with mRID, {mrid}, is not a cim.PowerElectronicsConnection. The '
                                 f'object is a {type(cimObj)}. This application will ignore sending a setpoint to this '
                                 'object.')
        setpointMessage = self.differenceBuilder.get_message()
        self.gad_obj.send(self.setpoints_topic, setpointMessage)

    def simulation_completed(self, sim: Simulation):
        self.log.info(f'Simulation for PeakShavingController:{self.id} has finished. This application '
                      'instance can be deleted.')
        self.isValid = False


def findMeasurement(measurementsList, type: str, phases):
    rv = None
    for measurement in measurementsList:
        if measurement.measurementType == type and measurement.phases in phases:
            rv = measurement
            break
    if rv is None:
        raise RuntimeError(f'findMeasurement(): No measurement of type {type} exists for any of the given phases!.')
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
        keys = list(powerTransformerEnds.keys())
        feederPowerRating = float(powerTransformerEnds[keys[0]].ratedS)
    else:
        raise RuntimeError('findFeederPowerRating(): The found at the feeder head is not a three phase transformer!')
    return feederPowerRating


def findFeederTransformer(cimObj):
    '''
        Helper function to find feeder transformer
    '''
    if not isinstance(cimObj, cim.EnergySource):
        raise TypeError('findFeederTransformer(): cimObj must be an instance of cim.EnergySource!')
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
    if not powerTransformerEnds:
        raise RuntimeError('findFeederPowerRating(): The found at the feeder head is not a three phase transformer!')
    return xfmr


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
    # Create an peak shaving controller instance for all the real systems in the database
    # for m in models:
    #     m_id = m.get('modelId')
    #     app_instances['field_instances'][m_id] = PeakShavingController(gad_object, m_id)
    for sim_id, m_id in platform_simulations.items():
        app_instances['external_simulation_instances'][sim_id] = PeakShavingController(gad_object, m_id, sim_id=sim_id)
    for m_id, simulation in local_simulations.items():
        app_instances['local_simulation_instances'][m_id] = PeakShavingController(gad_object,
                                                                                  m_id,
                                                                                  simulation=simulation)
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
                    gad_log.info('There have been no running instances PeakShavingController '
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
