import importlib
from argparse import ArgumentParser
from typing import Any, Dict

import cimgraph.utils as cimUtils
from cimgraph.databases import ConnectionParameters
from cimgraph.databases.blazegraph.blazegraph import BlazegraphConnection
from cimgraph.models import FeederModel
from gridappsd import GridAPPSD, topics


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
    perod = 600

    def __init__(self, period: int = None, sim_id: str = None, model_id: str = None):
        if not isinstance(period, int) and period is not None:
            raise TypeError(f'The period must be an int type or {None}!')
        if not isinstance(sim_id, str) and sim_id is not None:
            raise TypeError(f'The simulation id must be a string type or {None}!')
        self.cim_profile = 'rc4_2021'
        self.iec61970_301 = 7
        self.cim = importlib.import_module(f'cimgraph.data_profile.{self.cim_profile}')
        params = ConnectionParameters(url='http://localhost:8889/bigdata/namespace/kb/sparql',
                                      cim_profile=self.cim_profile,
                                      iec61970_301=self.iec61970_301)
        self.db_connection = BlazegraphConnection(params)
        feeder_container = self.cim.Feeder(mRID=model_id)
        self.graph_model = FeederModel(connection=self.db_connection, container=feeder_container, distributed=False)
        self.platform_measurements = {}
        self.last_setpoints = {}
        self.desired_setpoints = {}
        self.controllable_regulators = {}
        self.controllable_capacitors = []
        self.measurements = {}
        self.gapps = GridAPPSD(simulation_id=sim_id)
        self.measurements_topic = None
        if sim_id is None:
            measurements_topic = topics.field_output_topic(None, sim_id)
        else:
            measurements_topic = topics.simulation_output_topic(sim_id)
        self.gapps.subscribe(measurements_topic, self.on_measurement)
        #read model_id from cimgraph to get all the controllable regulators and capacitors.
        self.graph_model.get_all_edges(self.cim.PowerTransformer)
        self.graph_model.get_all_edges(self.cim.TransformerTank)
        self.graph_model.get_all_edges(self.cim.Asset)
        self.graph_model.get_all_edges(self.cim.LinearShuntCompensator)
        self.graph_model.get_all_edges(self.cim.PowerTransformerEnd)
        self.graph_model.get_all_edges(self.cim.TransformerEnd)
        self.graph_model.get_all_edges(self.cim.TransformerMeshImpedance)
        self.graph_model.get_all_edges(self.cim.TransformerTankEnd)
        self.graph_model.get_all_edges(self.cim.TransformerTankInfo)
        self.graph_model.get_all_edges(self.cim.LinearShuntCompensatorPhase)
        self.graph_model.get_all_edges(self.cim.Terminal)
        self.graph_model.get_all_edges(self.cim.ConnectivityNode)
        self.graph_model.get_all_edges(self.cim.BaseVoltage)
        self.graph_model.get_all_edges(self.cim.TransformerEndInfo)
        self.graph_model.get_all_edges(self.cim.Analog)
        self.graph_model.get_all_edges(self.cim.Discrete)
        #Store controllable_capacitors
        self.controllable_capacitors = list(self.graph_model.graph.get(self.cim.LinearShuntCompensator, {}))
        #Store controllable_regulators
        powerTransformers = self.graph_model.graph.get(self.cim.PowerTransformer, {})
        for mRID, powerTransformer in powerTransformers.items():
            for transformerTank in powerTransformer.TransformerTanks:
                for transformerEnd in transformerTank.TransformerTankEnds:
                    if transformerEnd.RatioTapChanger is not None:
                        if mRID not in self.controllable_regulators.keys():
                            self.controllable_regulators[mRID] = {}

        #TODO: read model from cimgraph to get end of line voltages.

    def on_measurement(self, headers: Dict[Any], message: Dict[Any]):
        #TODO: Updated self.platform_measurements with latest measurements from measurement topic.
        pass


def _main(model_id: str = None):
    #TODO: connect to the running GridAPPS-D platfrom
    global gapps_main, logger_main
    gapps_main = None
    if not isinstance(model_id, str) and model_id is not None:
        raise TypeError(
            f'The model id passed to the convervation voltage reduction application must be a string type or {None}.')
    try:
        gapps_main = GridAPPSD()
    except Exception as e:
        raise RuntimeError(
            f'The conservation voltage reduction application failed to connect to the GridAPPSD platform!. Platform connection error: \n{e}'
        )


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('model_id', narg='?', default=None, help='The mrid of the cim model to perform cvr on.')
    args = parser.parse_args()
    _main(args.model_id)
