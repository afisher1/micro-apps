import networkx as nx
import cimgraph.utils
import cimgraph.models
import cimgraph.data_profile.rc4_2021 as cim_dp
import models
import logging

log = logging.getLogger(__name__)


def generate_graph(network: cimgraph.models.GraphModel) -> (nx.Graph, str, str):
    graph = nx.Graph()

    network.get_all_edges(cim_dp.ACLineSegment)
    network.get_all_edges(cim_dp.EnergySource)
    network.get_all_edges(cim_dp.ConnectivityNode)
    network.get_all_edges(cim_dp.Terminal)

    source_bus = ""
    next_bus = ""
    mrid = ""
    source: cim_dp.EnergySource
    for source in network.graph[cim_dp.EnergySource].values():
        terminals = source.Terminals
        bus_1 = terminals[0].ConnectivityNode.name
        source_bus = bus_1

    xfmr: cim_dp.PowerTransformer
    for xfmr in network.graph[cim_dp.PowerTransformer].values():
        ends = xfmr.Terminals
        buses = []
        for end in ends:
            buses.append(end.ConnectivityNode.name)

        buses = list(set(buses))
        bus_1 = buses[0]
        bus_2 = buses[-1]
        if source_bus == bus_1:
            mrid = xfmr.mRID
            next_bus = bus_2
        if source_bus == bus_2:
            mrid = xfmr.mRID
            next_bus = bus_1
        graph.add_edge(bus_1, bus_2, weight=0.0)

    line: cim_dp.ACLineSegment
    for line in network.graph[cim_dp.ACLineSegment].values():
        terminals = line.Terminals
        bus_1 = terminals[0].ConnectivityNode.name
        bus_2 = terminals[1].ConnectivityNode.name
        if next_bus == bus_1:
            mrid = line.mRID
        if next_bus == bus_2:
            mrid = line.mRID
        graph.add_edge(bus_1, bus_2, weight=float(line.length))

    switch: cim_dp.LoadBreakSwitch
    for switch in network.graph[cim_dp.LoadBreakSwitch].values():
        # if not switch.normalOpen:
        terminals = switch.Terminals
        bus_1 = terminals[0].ConnectivityNode.name
        bus_2 = terminals[1].ConnectivityNode.name
        if source_bus == bus_1:
            mrid = switch.mRID
        if source_bus == bus_2:
            mrid = switch.mRID
        graph.add_edge(bus_1, bus_2, weight=0.0)

    return (graph, source_bus, mrid)


def get_graph_positions(network: cimgraph.models.GraphModel) -> dict:
    loc = cimgraph.utils.get_all_bus_locations(network)
    return {d['name']: [float(d['x']), float(d['y'])]
            for d in loc.values()}


def get_compensators(network: cimgraph.models.GraphModel) -> models.Compensators:
    compensators = models.Compensators()
    if cim_dp.LinearShuntCompensator not in network.graph:
        return compensators

    shunt: cim_dp.LinearShuntCompensator
    for shunt in network.graph[cim_dp.LinearShuntCompensator].values():

        mrid = shunt.mRID
        nom_v = float(shunt.nomU)
        p_imag = -float(shunt.bPerSection) * nom_v**2

        if not shunt.ShuntCompensatorPhase:
            measurement = cim_dp.Measurement
            for measurement in shunt.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                compensators.measurement_map[measurement.mRID] = info

            compensators.measurements_va[mrid] = models.PhasePower()
            compensators.measurements_pnv[mrid] = models.PhaseMap()

            power = models.ComplexPower(real=0.0, imag=p_imag/3)
            phases = models.PhasePower(a=power, b=power, c=power)
            compensators.ratings[mrid] = phases

        else:
            ratings = models.PhasePower()
            phase: models.LinearShuntCompensatorPhase
            for phase in shunt.ShuntCompensatorPhase:
                power = models.ComplexPower(real=0.0, imag=p_imag)
                if phase.phase == cim_dp.SinglePhaseKind.A:
                    ratings.a = power
                if phase.phase == cim_dp.SinglePhaseKind.B:
                    ratings.b = power
                if phase.phase == cim_dp.SinglePhaseKind.C:
                    ratings.c = power

            compensators.ratings[mrid] = ratings

            measurement = cim_dp.Measurement
            for measurement in shunt.Measurements:
                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                if measurement.measurementType == "PNV":
                    s1 = measurement.phases == cim_dp.PhaseCode.s1
                    s2 = measurement.phases == cim_dp.PhaseCode.s2
                    if not (s1 or s2):
                        print("IGNORING: ", mrid, measurement.name,
                              measurement.measurementType, measurement.phases)
                        continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                compensators.measurement_map[measurement.mRID] = info

            compensators.measurements_va[mrid] = models.PhasePower()
            compensators.measurements_pnv[mrid] = models.PhaseMap()

    return compensators


def get_consumers(network: cimgraph.GraphModel) -> models.Consumers:
    consumers = models.Consumers()
    if cim_dp.EnergyConsumer not in network.graph:
        return consumers

    load: cim_dp.EnergyConsumer
    for load in network.graph[cim_dp.EnergyConsumer].values():
        mrid = load.mRID

        if not load.EnergyConsumerPhase:
            measurement = cim_dp.Measurement
            for measurement in load.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                consumers.measurement_map[measurement.mRID] = info

            consumers.measurements_va[mrid] = models.PhasePower()
            consumers.measurements_pnv[mrid] = models.PhaseMap()

            p = float(load.p)
            q = float(load.q)
            power = models.ComplexPower(real=p/3, imag=q/3)
            ratings = models.PhasePower(a=power, b=power, c=power)
            consumers.ratings[mrid] = ratings

        else:
            ratings = models.PhasePower()
            phase: cim_dp.EnergyConsumerPhase

            for phase in load.EnergyConsumerPhase:
                power = models.ComplexPower(real=phase.p, imag=phase.q)

                if phase.phase == cim_dp.SinglePhaseKind.A:
                    ratings.a = power
                if phase.phase == cim_dp.SinglePhaseKind.B:
                    ratings.b = power
                if phase.phase == cim_dp.SinglePhaseKind.C:
                    ratings.c = power

            consumers.ratings[mrid] = ratings

            measurement = cim_dp.Measurement
            for measurement in load.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                if measurement.measurementType == "PNV":
                    s1 = measurement.phases == cim_dp.PhaseCode.s1
                    s2 = measurement.phases == cim_dp.PhaseCode.s2
                    if not (s1 or s2):
                        print("IGNORING: ", mrid, measurement.name,
                              measurement.measurementType, measurement.phases)
                        continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                consumers.measurement_map[measurement.mRID] = info

            consumers.measurements_va[mrid] = models.PhasePower()
            consumers.measurements_pnv[mrid] = models.PhaseMap()

    return consumers


def get_switches(network: cimgraph.GraphModel) -> models.Switches:
    switches = models.Switches()
    if cim_dp.LoadBreakSwitch not in network.graph:
        return switches

    switch: cim_dp.LoadBreakSwitch
    for switch in network.graph[cim_dp.LoadBreakSwitch].values():
        mrid = switch.mRID

        if not switch.SwitchPhase:
            measurement = cim_dp.Measurement
            for measurement in switch.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                switches.measurement_map[measurement.mRID] = info

            switches.measurements_va[mrid] = models.PhaseSwitch()
            switches.measurements_pnv[mrid] = models.PhaseMap()

            value = str(switch.normalOpen).capitalize() == "True"
            switches.normal[mrid] = models.PhaseSwitch(
                a=value, b=value, c=value)
        else:
            normal = models.PhaseSwitch()
            phase: cim_dp.SwitchPhase
            for phase in switch.SwitchPhase:
                value = str(switch.normalOpen).capitalize() == "True"
                if phase.phaseSide1 == cim_dp.SinglePhaseKind.A:
                    normal.a = value
                if phase.phaseSide1 == cim_dp.SinglePhaseKind.B:
                    normal.b = value
                if phase.phaseSide1 == cim_dp.SinglePhaseKind.C:
                    normal.c = value
            switches.normal[mrid] = normal

            measurement = cim_dp.Measurement
            for measurement in switch.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                if measurement.measurementType == "PNV":
                    s1 = measurement.phases == cim_dp.PhaseCode.s1
                    s2 = measurement.phases == cim_dp.PhaseCode.s2
                    if not (s1 or s2):
                        print("IGNORING: ", mrid, measurement.name,
                              measurement.measurementType, measurement.phases)
                        continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                switches.measurement_map[measurement.mRID] = info

            switches.measurements_va[mrid] = models.PhaseSwitch()
            switches.measurements_pnv[mrid] = models.PhaseMap()

    return switches


def get_power_electronics(network: cimgraph.GraphModel) -> models.PowerElectronics:
    electronics = models.PowerElectronics()
    if cim_dp.PowerElectronicsConnection not in network.graph:
        return electronics

    pec: cim_dp.PowerElectronicsConnection
    for pec in network.graph[cim_dp.PowerElectronicsConnection].values():
        mrid = pec.mRID

        for unit in pec.PowerElectronicsUnit:
            electronics.units[mrid] = unit.mRID

        if not pec.PowerElectronicsConnectionPhases:
            measurement = cim_dp.Measurement
            for measurement in pec.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                electronics.measurement_map[measurement.mRID] = info

            electronics.measurements_va[mrid] = models.PhasePower()
            electronics.measurements_pnv[mrid] = models.PhaseMap()

            rated_s = float(pec.ratedS)
            power = models.ComplexPower(real=rated_s/3, imag=rated_s/3)
            ratings = models.PhasePower(a=power, b=power, c=power)
            electronics.ratings[mrid] = ratings

        else:
            ratings = models.PhasePower()
            phase: cim_dp.PowerElectronicsConnectionPhase
            for phase in pec.PowerElectronicsConnectionPhases:
                power = models.ComplexPower(
                    real=float(phase.p), imag=float(phase.q))
                if phase.phase == cim_dp.SinglePhaseKind.A:
                    ratings.a = power
                if phase.phase == cim_dp.SinglePhaseKind.B:
                    ratings.b = power
                if phase.phase == cim_dp.SinglePhaseKind.C:
                    ratings.c = power
            electronics.ratings[mrid] = ratings

            measurement = cim_dp.Measurement
            for measurement in pec.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                if measurement.measurementType == "PNV":
                    s1 = measurement.phases == cim_dp.PhaseCode.s1
                    s2 = measurement.phases == cim_dp.PhaseCode.s2
                    if not (s1 or s2):
                        print("IGNORING: ", mrid, measurement.name,
                              measurement.measurementType, measurement.phases)
                        continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                electronics.measurement_map[measurement.mRID] = info

            electronics.measurements_va[mrid] = models.PhasePower()
            electronics.measurements_pnv[mrid] = models.PhaseMap()

    return electronics


def get_Transformers(network: cimgraph.GraphModel) -> models.Transformers:
    transformers = models.Transformers()
    if cim_dp.PowerTransformer not in network.graph:
        return transformers

    xfmr: cim_dp.PowerTransformer
    for xfmr in network.graph[cim_dp.PowerTransformer].values():
        mrid = xfmr.mRID

        measurement = cim_dp.Measurement
        for measurement in xfmr.Measurements:
            pnv = measurement.measurementType == "PNV"
            va = measurement.measurementType == "VA"
            if not (pnv or va):
                continue

            info = models.MeasurementInfo(
                mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
            transformers.measurement_map[measurement.mRID] = info

            transformers.measurements_va[mrid] = models.PhasePower()
            transformers.measurements_pnv[mrid] = models.PhaseMap()

    return transformers


def get_power_electronics(network: cimgraph.GraphModel) -> models.PowerElectronics:
    electronics = models.PowerElectronics()
    if cim_dp.PowerElectronicsConnection not in network.graph:
        return electronics

    pec: cim_dp.PowerElectronicsConnection
    for pec in network.graph[cim_dp.PowerElectronicsConnection].values():
        mrid = pec.mRID

        for unit in pec.PowerElectronicsUnit:
            electronics.units[mrid] = unit.mRID

        if not pec.PowerElectronicsConnectionPhases:
            measurement = cim_dp.Measurement
            for measurement in pec.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                electronics.measurement_map[measurement.mRID] = info

            electronics.measurements_va[mrid] = models.PhasePower()
            electronics.measurements_pnv[mrid] = models.PhaseMap()

            rated_s = float(pec.ratedS)
            power = models.ComplexPower(real=rated_s/3, imag=rated_s/3)
            ratings = models.PhasePower(a=power, b=power, c=power)
            electronics.ratings[mrid] = ratings

        else:
            ratings = models.PhasePower()
            phase: cim_dp.PowerElectronicsConnectionPhase
            for phase in pec.PowerElectronicsConnectionPhases:
                power = models.ComplexPower(
                    real=float(phase.p), imag=float(phase.q))
                if phase.phase == cim_dp.SinglePhaseKind.A:
                    ratings.a = power
                if phase.phase == cim_dp.SinglePhaseKind.B:
                    ratings.b = power
                if phase.phase == cim_dp.SinglePhaseKind.C:
                    ratings.c = power
            electronics.ratings[mrid] = ratings

            measurement = cim_dp.Measurement
            for measurement in pec.Measurements:

                pnv = measurement.measurementType == "PNV"
                va = measurement.measurementType == "VA"
                if not (pnv or va):
                    continue

                if measurement.measurementType == "PNV":
                    s1 = measurement.phases == cim_dp.PhaseCode.s1
                    s2 = measurement.phases == cim_dp.PhaseCode.s2
                    if not (s1 or s2):
                        print("IGNORING: ", mrid, measurement.name,
                              measurement.measurementType, measurement.phases)
                        continue

                info = models.MeasurementInfo(
                    mrid=mrid, value_type=measurement.measurementType, phase=measurement.phases)
                electronics.measurement_map[measurement.mRID] = info

            electronics.measurements_va[mrid] = models.PhasePower()
            electronics.measurements_pnv[mrid] = models.PhaseMap()

    return electronics


def map_power_electronics(network: cimgraph.GraphModel) -> dict:
    map = {}
    if cim_dp.PowerElectronicsConnection not in network.graph:
        return map

    pec: cim_dp.PowerElectronicsConnection
    for pec in network.graph[cim_dp.PowerElectronicsConnection].values():
        name = pec.mRID
        bus = pec.Terminals[0].ConnectivityNode.name
        map[name] = bus
    return map
