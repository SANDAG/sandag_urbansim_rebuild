import os
import pandas as pd
import pandana as pdna
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim_defaults import models

#sim.broadcast('building_sqft_per_job', 'buildings', cast_index=True, onto_on=['luz_id','building_type_id'])

@sim.table('building_sqft_per_job', cache=True)
def building_sqft_per_job(store):
    return store['building_sqft_per_job']

@sim.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    bldgs = buildings.to_frame(['luz_id', 'building_type_id'])
    merge_df = pd.merge(bldgs, building_sqft_per_job.to_frame(), how='left', left_on=['luz_id', 'building_type_id'], right_index=True)
    merge_df.sqft_per_emp.fillna(-1, inplace=True)
    merge_df.loc[merge_df.sqft_per_emp < 40, 'sqft_per_emp'] = 40
    return merge_df.sqft_per_emp

@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)

@sim.column('parcels', 'parcel_acres')
def parcel_acres(parcels):
    return parcels.acres

@sim.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']

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

sim.run(['build_networks', 'neighborhood_vars'])

nodes = sim.get_table('nodes')
print nodes.to_frame()

#print nodes.toframe(nodes.local_columns)