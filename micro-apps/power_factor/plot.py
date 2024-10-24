import os
import json
import math
import traceback
import networkx as nx
import numpy as np
# from pandas import DataFrame
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from datetime import timedelta
from models import DataInfo, PhasePower, Data

ROOT = os.getcwd()
IN_DIR = f"{ROOT}/inputs"
OUT_DIR = f"{ROOT}/outputs"


def load_summary(filepath: str) -> DataInfo:
    with open(filepath) as f:
        data = json.load(f)
    return DataInfo(**data)


def extract_real(data: list[PhasePower]) -> (list[float], list[float], list[float]):
    a = [var['a']['real']/1000 for var in data]
    b = [var['b']['real']/1000 for var in data]
    c = [var['c']['real']/1000 for var in data]
    return (a, b, c)


def extract_imag(data: list[PhasePower]) -> (list[float], list[float], list[float]):
    a = [var['a']['imag']/1000 for var in data]
    b = [var['b']['imag']/1000 for var in data]
    c = [var['c']['imag']/1000 for var in data]
    return (a, b, c)


def extract_comps_real(data: dict[PhasePower]) -> (dict[float], dict[float], dict[float]):
    a = {}
    b = {}
    c = {}
    for k, v in data.items():
        if v['a']['real'] != 0.0:
            a[k] = v['a']['real']/1000
        if v['b']['real'] != 0.0:
            b[k] = v['b']['real']/1000
        if v['c']['real'] != 0.0:
            c[k] = v['c']['real']/1000
    return (a, b, c)


def extract_comps_imag(data: dict[PhasePower]) -> (dict[float], dict[float], dict[float]):
    a = {}
    b = {}
    c = {}
    for k, v in data.items():
        if v['a']['imag'] != 0.0:
            a[k] = v['a']['imag']/1000
        if v['b']['imag'] != 0.0:
            b[k] = v['b']['imag']/1000
        if v['c']['imag'] != 0.0:
            c[k] = v['c']['imag']/1000
    return (a, b, c)


def extract_pecs_real(data: dict[PhasePower]) -> (dict[float], dict[float], dict[float]):
    a = {}
    b = {}
    c = {}
    for k, v in data.items():
        if v['a']['real'] != 0.0:
            a[k] = v['a']['real']/1000
        if v['b']['real'] != 0.0:
            b[k] = v['b']['real']/1000
        if v['c']['real'] != 0.0:
            c[k] = v['c']['real']/1000
    return (a, b, c)


def extract_pecs_imag(data: dict[PhasePower]) -> (dict[float], dict[float], dict[float]):
    a = {}
    b = {}
    c = {}
    for k, v in data.items():
        if v['a']['imag'] != 0.0:
            a[k] = v['a']['imag']/1000
        if v['b']['imag'] != 0.0:
            b[k] = v['b']['imag']/1000
        if v['c']['imag'] != 0.0:
            c[k] = v['c']['imag']/1000
    return (a, b, c)


def percent_avail(s: float, p: float, q: float) -> float:
    if q == 0.0:
        return 0

    num = abs(q)
    den = math.sqrt(s**2 - p**2)
    return num/den


def gather_pecs(
        dist: dict[float],
        nameplate: dict[float],
        real: dict[float],
        imag: dict[float]) -> (list[float], list[float]):
    x = []
    y = []
    for k, d in dist.items():
        if k in nameplate:
            va = nameplate[k]

            w = 0.0
            if k in real:
                w = real[k]

            var = 0.0
            if k in imag:
                var = imag[k]

            y.append(percent_avail(va, w, var))
            x.append(d)
    return (x, y)


def plot_pecs(summary: DataInfo, idx: int) -> None:

    dist = data.pecs_distance
    (np_a, np_b, np_c) = extract_pecs_real(data.pecs_ratings)
    pecs = data.data[idx]['pecs']
    (imag_a, imag_b, imag_c) = extract_pecs_imag(pecs)
    (real_a, real_b, real_c) = extract_pecs_real(pecs)

    ax, ay = gather_pecs(dist, np_a, real_a, imag_a)
    bx, by = gather_pecs(dist, np_b, real_b, imag_b)
    cx, cy = gather_pecs(dist, np_c, real_c, imag_c)

    x = ax + bx + cx
    y = ay + by + cy

    fig, axis = plt.subplots()
    axis.scatter(x, y)

    axis.set(xlabel='Distance (km)', ylabel='Reactive Power (% avail)')
    plt.savefig(f'{OUT_DIR}/dist_var_{idx}.png', dpi=400)


def plot(summary: DataInfo) -> None:

    total_load = [data['total_load'] for data in summary.data[2:]]
    (total_a, total_b, total_c) = extract_imag(total_load)

    net_load = [data['net_load'] for data in summary.data[2:]]
    (net_a, net_b, net_c) = extract_imag(net_load)

    pecs = [data['pec_dispatch'] for data in summary.data[2:]]
    (pecs_a, pecs_b, pecs_c) = extract_imag(pecs)

    fig, ax = plt.subplots(3, sharex=True)
    ax[0].plot(range(len(total_a)), total_a, label='total')
    ax[0].plot(range(len(net_a)), net_a, label='net')
    ax[0].plot(range(len(pecs_a)), pecs_a, label='pecs')
    ax[1].plot(range(len(total_b)), total_b, label='total')
    ax[1].plot(range(len(net_b)), net_b, label='net')
    ax[1].plot(range(len(pecs_b)), pecs_b, label='pecs')
    ax[2].plot(range(len(total_c)), total_c, label='total')
    ax[2].plot(range(len(net_c)), net_c, label='net')
    ax[2].plot(range(len(pecs_c)), pecs_c, label='pecs')

    phases = ["A", "B", "C"]
    for idx, axis in enumerate(ax.flat):
        axis.set(xlabel='Time (H)', ylabel=f"{phases[idx]} (kVAR)")
        axis.label_outer()

    fig.legend(["Total", "Compensators", "PECs"], loc='upper center', ncols=3)
    plt.savefig(f'{OUT_DIR}/total_var.png', dpi=400)


def plot_graph(G: nx.Graph, dist: dict, pos: dict) -> None:

    # nodes
    nx.draw_networkx_nodes(
        G,
        pos,
        node_size=20,
        nodelist=list(dist.keys()),
        node_color=list(dist.values()),
        cmap=plt.cm.plasma
    )

    # node labels
    nx.draw_networkx_labels(G, pos, font_size=2, font_family="sans-serif")

    # edges
    nx.draw_networkx_edges(G, pos, width=2, alpha=0.4)

    # edge weight labels
    edge_labels = nx.get_edge_attributes(G, "weight")
    nx.draw_networkx_edge_labels(G, pos, edge_labels, font_size=2)

    ax = plt.gca()
    ax.margins(0.08)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig("outputs/graph.png", dpi=400)


def polygon_under_graph(x: list[float], y: list[float]):
    """
    Construct the vertex list which defines the polygon filling the space under
    the (x, y) line graph. This assumes x is in ascending order.
    """
    return [(x[0], 0.), *zip(x, y), (x[-1], 0.)]


def plot_3d(summary: DataInfo) -> None:
    dist = data.pecs_distance
    (np_a, np_b, np_c) = extract_pecs_real(data.pecs_ratings)

    axis = plt.figure().add_subplot(projection='3d')

    x = []
    y = []
    z = []
    for step in data.data:
        ts = step['timestamp']
        if ts % 3600 != 0:
            continue

        pecs = step['pecs']
        (imag_a, imag_b, imag_c) = extract_pecs_imag(pecs)
        (real_a, real_b, real_c) = extract_pecs_real(pecs)
        ax, ay = gather_pecs(dist, np_a, real_a, imag_a)
        bx, by = gather_pecs(dist, np_b, real_b, imag_b)
        cx, cy = gather_pecs(dist, np_c, real_c, imag_c)
        combined = list(zip(ax+bx+cx, ay+by+cy))
        sorted_combined = sorted(combined)
        d, q = zip(*sorted_combined)
        x.append(d)
        y.append(q)
        z.append(timedelta(seconds=ts).seconds//3600)

    verts = [polygon_under_graph(a, b) for a, b in zip(x, y)]
    facecolors = plt.colormaps['viridis_r'](np.linspace(0, 1, len(verts)))
    poly = PolyCollection(verts, facecolors=facecolors, alpha=.25)
    axis.add_collection3d(poly, zs=z, zdir='y')
    for idx, hour in enumerate(z):
        z = [hour]*len(x[idx])
        axis.plot(x[idx], hour, y[idx], c=facecolors[idx], alpha=1)

    axis.set(
        xlabel='Dist (km)',
        zlabel='VAR (%)',
        ylabel='Time (HH)')
    axis.invert_xaxis()

    plt.savefig(f'{OUT_DIR}/pecs_3d.png', dpi=400)


if __name__ == "__main__":
    try:
        data = load_summary(f"{OUT_DIR}/summary.json")
        plot(data)
        plot_3d(data)

    except Exception as e:
        print(e)
        print(traceback.format_exc())
