import numpy as np
import os
import pandas as pd
import pandana as pdna
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim_defaults import models

@sim.table('building_sqft_per_job', cache=True)
def building_sqft_per_job(store):
    return store['building_sqft_per_job']

@sim.table('scheduled_development_events', cache=True)
def scheduled_development_events(store):
    return store['scheduled_development_events']

@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)

@sim.column('buildings', 'building_sqft')
def building_sqft(buildings):
    return buildings.residential_sqft + buildings.non_residential_sqft

@sim.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    bldgs = buildings.to_frame(['luz_id', 'building_type_id'])
    merge_df = pd.merge(bldgs, building_sqft_per_job.to_frame(), how='left', left_on=['luz_id', 'building_type_id'], right_index=True)
    merge_df.sqft_per_emp.fillna(-1, inplace=True)
    merge_df.loc[merge_df.sqft_per_emp < 40, 'sqft_per_emp'] = 40
    return merge_df.sqft_per_emp

@sim.column('nodes', 'nonres_occupancy_3000m')
def nonres_occupancy_3000m(nodes):
    return nodes.jobs_3000m / (nodes.job_spaces_3000m + 1.0)

@sim.column('parcels', 'parcel_acres')
def parcel_acres(parcels):
    return parcels.acres

@sim.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']

def get_year():
    year = sim.get_injectable('year')
    if year is None:
        year = 2015
    return year

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
    #p.to_csv('data/parcels.csv')
    sim.add_table("parcels", p)

@sim.model('scheduled_development_events')
def scheduled_development_events(scheduled_development_events, buildings):
    year = get_year()
    sched_dev = scheduled_development_events.to_frame()
    sched_dev = sched_dev[sched_dev.year_built==year]
    sched_dev['residential_sqft'] = sched_dev.sqft_per_unit*sched_dev.residential_units
    #TODO: The simple division here is not consistent with other job_spaces calculations
    sched_dev['job_spaces'] = sched_dev.non_residential_sqft/400
    if len(sched_dev) > 0:
        max_bid = buildings.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['building_id'] = idx
        sched_dev = sched_dev.set_index('building_id')
        from urbansim.developer.developer import Developer
        merge = Developer(pd.DataFrame({})).merge
        b = buildings.to_frame(buildings.local_columns)
        all_buildings = merge(b,sched_dev[b.columns])
        sim.add_table("buildings", all_buildings)


sim.run(['build_networks'])

sim.run(['scheduled_development_events', 'neighborhood_vars'
         #,'rsh_simulate'
         ,'nrh_simulate']) #, years=xrange(2015,2015))

nodes = sim.get_table('nodes')

results_df = nodes.to_frame()

results_df.to_csv('data/results.csv')


print results_df[results_df.node_id == 87868]

#sim.get_table('parcels').to_frame().to_csv('data/parcels.csv')



