import os
import json
import traceback
import logging
import importlib
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, asdict

import cvxpy as cp
import networkx as nx
import numpy as np

from cimgraph.databases import BlazegraphConnection, ConnectionParameters, GridappsdConnection
from cimgraph.models import FeederModel
from gridappsd import GridAPPSD, DifferenceBuilder
from gridappsd import topics as t
from gridappsd.simulation import ApplicationConfig, ModelCreationConfig, PowerSystemConfig, ServiceConfig, Simulation, \
    SimulationArgs, SimulationConfig

import cimgraph.utils
import cimgraph.data_profile.cimhub_2023 as cim_dp

import models
import query

IEEE123_APPS = "E3D03A27-B988-4D79-BFAB-F9D37FB289F7"
IEEE13 = "49AD8E07-3BF9-A4E2-CB8F-C3722F837B62"
IEEE123 = "C1C3E687-6FFD-C753-582B-632A27E28507"
IEEE123PV = "E407CBB6-8C8D-9BC9-589C-AB83FBF0826D"
SOURCE_BUS = "150r"
OUT_DIR = "outputs"
ROOT = os.getcwd()


logging.basicConfig(level=logging.DEBUG,
                    filename=f"{ROOT}/{OUT_DIR}/log.txt", filemode='w')
log = logging.getLogger(__name__)


def remap_phases(
        info: models.MeasurementInfo,
        map: models.PhaseMap) -> models.MeasurementInfo:
    if info.phase == cim_dp.PhaseCode.s1:
        info.phase = map.s1
        return info

    if info.phase == cim_dp.PhaseCode.s2:
        info.phase = map.s2
        return info

    return info


def dist_matrix(ders: dict, dists: dict) -> np.ndarray:
    phases = 3
    map = [1/(1+dists[bus]) for der, bus in ders.items()]
    map = map / np.linalg.norm(map)
    mat = np.array([map,]*phases).transpose()
    return mat


def dict_to_array(d: dict) -> np.ndarray:
    results = d.values()
    data = list(results)
    return np.array(data)


def save_data(results: models.DataInfo,  filename: str) -> None:
    with open(f"{OUT_DIR}/{filename}.json", "w") as f:
        f.write(json.dumps(asdict(results)))


class PowerFactor(object):
    gapps: GridAPPSD
    sim: Simulation
    network: cimgraph.GraphModel
    compensators: models.Compensators
    consumers: models.Consumers
    electronics: models.PowerElectronics
    source_mrid: str
    data_info: models.DataInfo

    def __init__(self, gapps: GridAPPSD, sim: Simulation, network: cimgraph.GraphModel):
        self.simulation = sim
        self.gapps = gapps
        self.switches = query.get_switches(network)
        self.compensators = query.get_compensators(network)
        self.consumers = query.get_consumers(network)
        self.electronics = query.get_power_electronics(network)
        self.tranformers = query.get_Transformers(network)
        self.data_info = models.DataInfo()

        self.data_info.compensators_ratings = self.compensators.ratings
        self.data_info.pecs_ratings = self.electronics.ratings
        self.temp_source_bus = ["43c1d677-586e-4ef5-b35c-f08d326af31c",
                                "cd3e1fca-6826-4d8b-8d42-5cb5817e085f",
                                "36e2d53e-6e91-4a0c-a1ce-a196ff422332"]

        (graph, source_bus, mrid) = query.generate_graph(network)
        self.source_mrid = mrid
        paths = nx.shortest_path_length(graph, source=source_bus)
        pec_map = query.map_power_electronics(network)
        self.dist = dist_matrix(pec_map, paths)
        self.data_info.pecs_distance = {k: (1/v[0]) - 1
                                        for k, v in zip(pec_map.keys(), self.dist)}

        sim.add_onmeasurement_callback(self.on_measurement)
        self.diff_builder = DifferenceBuilder(sim.simulation_id)
        self.topic = t.simulation_input_topic(sim.simulation_id)
        self.last_dispatch = 0

        # self.init_electronics()

    def on_measurement(self, sim: Simulation, timestamp: dict, measurements: dict) -> None:
        print(f"{timestamp}: on_measurement")
        if timestamp - self.last_dispatch >= 6:

            pecs = {key: models.PhasePower()
                    for key in self.electronics.ratings.keys()}

            self.data_info.data.append(models.Data(
                timestamp=timestamp,
                total_load=models.PhasePower(),
                net_load=models.PhasePower(),
                pec_dispatch=models.PhasePower(),
                pecs=pecs
            ))
            self.last_dispatch = timestamp
            control = self.dispatch()
            print("Total Reactive PECS: ", np.sum(control, axis=0))
            self.set_electronics(control)

        for k, v in measurements.items():
            if k in self.temp_source_bus:
                pass
                # print("SOURCE: ", k, v)

            if k in self.switches.measurement_map:
                val = models.SimulationMeasurement(**v)
                if val.value is not None:
                    info = self.switches.measurement_map[k]

            if k in self.consumers.measurement_map:
                val = models.SimulationMeasurement(**v)
                if val.value is not None:
                    continue

                info = self.consumers.measurement_map[k]
                if info.value_type == "PNV":
                    old = self.consumers.measurements_pnv[info.mrid]
                    new = models.update_pnv(info, val, old)
                    self.consumers.measurements_pnv[info.mrid] = new

                if info.value_type == "VA":
                    map = self.consumers.measurements_pnv[info.mrid]
                    info = remap_phases(info, map)
                    self.consumers.measurement_map[k] = info

                    old = self.consumers.measurements_va[info.mrid]
                    new = models.update_va(info, val, old)
                    self.consumers.measurements_va[info.mrid] = new

            if k in self.compensators.measurement_map:
                val = models.SimulationMeasurement(**v)
                if val.value is not None:
                    continue

                info = self.compensators.measurement_map[k]
                if info.value_type == "PNV":
                    old = self.compensators.measurements_pnv[info.mrid]
                    new = models.update_pnv(info, val, old)
                    self.compensators.measurements_pnv[info.mrid] = new

                if info.value_type == "VA":
                    map = self.compensators.measurements_pnv[info.mrid]
                    info = remap_phases(info, map)
                    self.compensators.measurement_map[k] = info

                    old = self.compensators.measurements_va[info.mrid]
                    new = models.update_va(info, val, old)
                    self.compensators.measurements_va[info.mrid] = new

            if k in self.tranformers.measurement_map:
                val = models.SimulationMeasurement(**v)
                if val.value is not None:
                    continue

                info = self.tranformers.measurement_map[k]
                if info.value_type == "PNV":
                    old = self.tranformers.measurements_pnv[info.mrid]
                    new = models.update_pnv(info, val, old)
                    self.tranformers.measurements_pnv[info.mrid] = new

                if info.value_type == "VA":
                    map = self.tranformers.measurements_pnv[info.mrid]
                    info = remap_phases(info, map)
                    self.tranformers.measurement_map[k] = info

                    old = self.tranformers.measurements_va[info.mrid]
                    new = models.update_va(info, val, old)
                    self.tranformers.measurements_va[info.mrid] = new

            if k in self.electronics.measurement_map:
                val = models.SimulationMeasurement(**v)
                if val.value is not None:
                    continue

                info = self.electronics.measurement_map[k]
                if info.value_type == "PNV":
                    old = self.electronics.measurements_pnv[info.mrid]
                    new = models.update_pnv(info, val, old)
                    self.electronics.measurements_pnv[info.mrid] = new

                if info.value_type == "VA":
                    map = self.electronics.measurements_pnv[info.mrid]
                    info = remap_phases(info, map)
                    self.electronics.measurement_map[k] = info

                    old = self.electronics.measurements_va[info.mrid]
                    new = models.update_va(info, val, old)
                    self.electronics.measurements_va[info.mrid] = new
        if self.simulation is not None:
            self.simulation.resume()

    def toggle_switches(self) -> None:
        v: models.MeasurementInfo
        for k, v in self.switches.measurement_map.items():
            if v.phase == "A":
                self.diff_builder.add_difference(
                    k, models.Switches.open_attribute, True, False
                )

        topic = t.simulation_input_topic(sim.simulation_id)
        message = self.diff_builder.get_message()
        # self.gapps.send(topic, message)

    def set_compensators(self) -> np.ndarray:
        real_loads = dict_to_array(self.consumers.measurements_va)[:, :, 0]
        total_real = np.sum(real_loads, axis=0).tolist()

        imag_loads = dict_to_array(self.consumers.measurements_va)[:, :, 1]
        total_imag = np.sum(imag_loads, axis=0).tolist()

        starting_load = np.sum(imag_loads, axis=0)
        total_load = np.sum(imag_loads, axis=0)

        self.data_info.data[-1].total_load.set_phases(total_real, total_imag)

        comps_abc = {k: v for k, v in self.compensators.ratings.items()
                     if v.a.imag != 0 and v.b.imag != 0 and v.c.imag != 0}
        comps_a = {k: v for k, v in self.compensators.ratings.items()
                   if v.b.imag == 0 and v.c.imag == 0}
        comps_b = {k: v for k, v in self.compensators.ratings.items()
                   if v.a.imag == 0 and v.c.imag == 0}
        comps_c = {k: v for k, v in self.compensators.ratings.items()
                   if v.a.imag == 0 and v.b.imag == 0}

        for k, v in comps_abc.items():
            vars = np.sum(np.array(v), axis=1)
            net = total_load + vars
            if net.all() < 0:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 0, 1
                )
            else:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 1, 0
                )
                total_load = net

        for k, v in comps_a.items():
            vars = np.sum(np.array(v), axis=1)
            net = total_load + vars
            if net[0] < 0:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 0, 1
                )
            else:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 1, 0
                )
                total_load = net

        for k, v in comps_b.items():
            vars = np.sum(np.array(v), axis=1)
            net = total_load + vars
            if net[1] < 0:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 0, 1
                )
            else:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 1, 0
                )
                total_load = net

        for k, v in comps_c.items():
            vars = np.sum(np.array(v), axis=1)
            net = total_load + vars
            if net[2] < 0:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 0, 1
                )
            else:
                self.diff_builder.add_difference(
                    k, models.Compensators.sections_attribute, 1, 0
                )
                total_load = net

        topic = t.simulation_input_topic(sim.simulation_id)
        message = self.diff_builder.get_message()
        # self.gapps.send(topic, message)
        net = starting_load - total_load
        self.data_info.data[-1].net_load.set_phases(
            total_real, net.tolist())
        return total_load

    def init_electronics(self) -> None:
        for idx, key in enumerate(self.electronics.ratings.keys()):
            mrid = self.electronics.units[key]
            self.diff_builder.add_difference(
                mrid, models.PowerElectronics.p_attribute, 0.0, 0.0
            )
        topic = t.simulation_input_topic(sim.simulation_id)
        message = self.diff_builder.get_message()
        # self.gapps.send(topic, message)

    def set_electronics(self, dispatch: np.ndarray) -> None:
        real = dict_to_array(self.electronics.measurements_va)[:, :, 0]
        total_real = np.sum(real, axis=0)
        total_imag = np.sum(dispatch, axis=0)
        self.data_info.data[-1].pec_dispatch.set_phases(total_real, total_imag)

        for idx, key in enumerate(self.electronics.ratings.keys()):
            mrid = self.electronics.units[key]
            self.data_info.data[-1].pecs[key].set_phases(
                real[idx], dispatch[idx].tolist())
            value = np.sum(dispatch[idx]).item()
            self.diff_builder.add_difference(
                mrid, models.PowerElectronics.q_attribute, value, 0.0
            )
        topic = t.simulation_input_topic(sim.simulation_id)
        message = self.diff_builder.get_message()
        # self.gapps.send(topic, message)

    def get_phase_vars(self) -> np.ndarray:
        load = dict_to_array(self.consumers.measurements_va)[:, :, 1]
        total_load = np.sum(load, axis=0)

        comp = dict_to_array(self.compensators.measurements_va)[:, :, 1]
        total_comp = np.sum(comp, axis=0)

        print("Total consumers: ", total_load)
        print("Total compensators: ", total_comp)

        return total_load + total_comp

    def get_bounds(self) -> np.ndarray:
        apparent = dict_to_array(self.electronics.ratings)[:, :, 0]
        real = dict_to_array(self.electronics.measurements_va)[:, :, 0]
        total_real = np.sum(real, axis=0).tolist()
        print("Total Apparent PECS: ", np.sum(apparent, axis=0))
        print("Total Real PECS: ", np.sum(real, axis=0))
        reactive = np.sqrt(abs(real**2 - apparent**2))
        return reactive

    def dispatch(self) -> np.ndarray:
        bounds = self.get_bounds()
        A = np.clip(bounds, a_min=0, a_max=1)
        b = self.set_compensators()

        # constrain values to lesser of ders and network
        total_der = np.sum(bounds, axis=0)
        direction = np.clip(b, a_max=1, a_min=-1)
        b = np.array([min(vals) for vals in zip(abs(total_der), abs(b))])
        b = b*direction

        # Construct the problem.
        m, n = np.shape(A)
        x = cp.Variable((m, n))

        cost = cp.sum(self.dist.T@cp.abs(x))
        objective = cp.Minimize(cost)
        constraints = [
            cp.sum(A.T@x, axis=0) == b,
            -bounds <= x,
            x <= bounds]

        prob = cp.Problem(objective, constraints)

        # The optimal objective is returned by prob.solve().
        prob.solve(solver=cp.CLARABEL, verbose=False)

        if prob.status == 'optimal':
            return np.round(x.value, 0)

        return np.zeros_like(x)


@dataclass
class ModelInfo:
    modelName: str
    modelId: str
    stationName: str
    stationId: str
    subRegionName: str
    subRegionId: str
    regionName: str
    regionId: str


if __name__ == "__main__":
    try:
        cim_profile = 'rc4_2021'
        cim = importlib.import_module('cimgraph.data_profile.' + cim_profile)
        mrid = IEEE123PV

        feeder = cim.Feeder(mRID=mrid)

        params = ConnectionParameters(
            url="http://localhost:8889/bigdata/namespace/kb/sparql",
            cim_profile=cim_profile)
        bgc = BlazegraphConnection(params)

        # params = ConnectionParameters(
        #    cim_profile=cim_profile,
        #    iec61970_301=7
        # )
        # gac = GridappsdConnection(params)
        gapps = GridAPPSD(username='system', password='manager')
        network = FeederModel(
            connection=bgc,
            container=feeder,
            distributed=False)

        cimgraph.utils.get_all_data(network)
        cimgraph.utils.get_all_measurement_data(network)

        model_info = gapps.query_model_info()['data']['models']
        for model in model_info:
            if model['modelId'] == mrid:
                system = ModelInfo(**model)

        system_config = PowerSystemConfig(
            GeographicalRegion_name=system.regionId,
            SubGeographicalRegion_name=system.subRegionId,
            Line_name=system.modelId
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

        start = datetime(2023, 1, 1, 4)
        epoch = datetime.timestamp(start)
        duration = timedelta(hours=24).total_seconds()

        start = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
        epoch = datetime.timestamp(start)
        duration = timedelta(hours=1).total_seconds()
        sim_args = SimulationArgs(
            start_time=epoch,
            duration=duration,
            simulator="GridLAB-D",
            timestep_frequency=1000,
            timestep_increment=1000,
            run_realtime=False,
            simulation_name=system.modelName,
            power_flow_solver_method="NR",
            model_creation_config=asdict(model_config),
            pause_after_measurements=True,
        )

        sim_config = SimulationConfig(
            power_system_config=asdict(system_config),
            simulation_config=asdict(sim_args)
        )
        sim = Simulation(gapps, asdict(sim_config))

        app = PowerFactor(gapps, sim, network)

        sim.run_loop()

        save_data(app.data_info, "summary")

    except KeyboardInterrupt:
        sim.stop()

    except Exception as e:
        log.debug(e)
        log.debug(traceback.format_exc())
