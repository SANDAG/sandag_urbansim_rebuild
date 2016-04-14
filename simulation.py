import os
import pandas as pd
import pandana as pdna
import urbansim.sim.simulation as sim
from urbansim_defaults import models

@sim.model('build_networks')
def build_networks(parcels):
    st = pd.HDFStore('data/urbansim.h5', "r")
    nodes, edges = st.nodes, st.edges
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])
    net.precompute(3000)
    sim.add_injectable("net", net)

    p = parcels.to_frame(parcels.local_columns)

    p['node_id'] = net.get_node_ids(p['x'], p['y'])
    sim.add_table("parcels", p)

sim.run(["build_networks"])

sim.run(['neighborhood_vars'])

nodes = sim.get_table('nodes')

print nodes.toframe(nodes.local_columns)