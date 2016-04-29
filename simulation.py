import numpy as np
import os
import pandas as pd
import pandana as pdna
from urbansim.models import transition
import urbansim.sim.simulation as sim
from urbansim.utils import misc
from urbansim_defaults import models
from urbansim_defaults import utils


@sim.table('building_sqft_per_job', cache=True)
def building_sqft_per_job(store):
    return store['building_sqft_per_job']


@sim.table('household_controls', cache=True)
def household_controls(store):
    return store['household_controls']


@sim.table('scheduled_development_events', cache=True)
def scheduled_development_events(store):
    return store['scheduled_development_events']


@sim.column('buildings', 'building_sqft')
def building_sqft(buildings):
    return buildings.residential_sqft + buildings.non_residential_sqft


@sim.column('buildings', 'distance_to_coast')
def distance_to_coaset(buildings, parcels):
    return misc.reindex(parcels.distance_to_coast, buildings.parcel_id)


@sim.column('buildings', 'distance_to_freeway')
def distance_to_freeway(buildings, parcels):
    return misc.reindex(parcels.distance_to_freeway, buildings.parcel_id)


@sim.column('buildings', 'distance_to_onramp')
def distance_to_onramp(settings, net, buildings):
    ramp_distance = settings['build_networks']['on_ramp_distance']
    distance_df = net.nearest_pois(ramp_distance, 'onramps', num_pois=1, max_distance=ramp_distance)
    distance_df.columns = ['distance_to_onramp']
    return misc.reindex(distance_df.distance_to_onramp, buildings.node_id)

@sim.column('buildings', 'distance_to_park')
def distance_to_park(settings, net, buildings):
    park_distance = settings['build_networks']['parks_distance']
    distance_df = net.nearest_pois(park_distance, 'parks', num_pois=1, max_distance=park_distance)
    distance_df.columns = ['distance_to_park']
    return misc.reindex(distance_df.distance_to_park, buildings.node_id)

@sim.column('buildings','distance_to_school')
def distance_to_school(settings, net, buildings):
    school_distance = settings['build_networks']['schools_distance']
    distance_df = net.nearest_pois(school_distance, 'schools', num_pois=1, max_distance=school_distance)
    distance_df.columns = ['distance_to_school']
    return misc.reindex(distance_df.distance_to_school, buildings.node_id)

@sim.column('buildings','distance_to_transit')
def distance_to_transit(settings, net, buildings):
    transit_distance = settings['build_networks']['transit_distance']
    distance_df = net.nearest_pois(transit_distance, 'transit', num_pois=1, max_distance=transit_distance)
    distance_df.columns = ['distance_to_transit']
    return misc.reindex(distance_df.distance_to_transit, buildings.node_id)


@sim.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id)


@sim.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    bldgs = buildings.to_frame(['luz_id', 'building_type_id'])
    merge_df = pd.merge(bldgs, building_sqft_per_job.to_frame(), how='left', left_on=['luz_id', 'building_type_id'], right_index=True)
    merge_df.sqft_per_emp.fillna(-1, inplace=True)
    merge_df.loc[merge_df.sqft_per_emp < 40, 'sqft_per_emp'] = 40
    return merge_df.sqft_per_emp


@sim.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0).astype('int64')


@sim.column('households', 'income_quartile', cache=True)
def income_quartile(households):
    hh_inc = households.to_frame(['household_id', 'income'])
    bins = [hh_inc.income.min()-1, 30000, 59999, 99999, 149999, hh_inc.max()+1]
    group_names = range(1,6)
    return pd.cut(hh_inc.income, bins, labels=group_names).astype('int64')


@sim.column('nodes', 'nonres_occupancy_3000m')
def nonres_occupancy_3000m(nodes):
    return nodes.jobs_3000m / (nodes.job_spaces_3000m + 1.0)

@sim.column('nodes', 'res_occupancy_3000m')
def res_occupancy_3000m(nodes):
    return nodes.households_3000m / (nodes.residential_units_3000m + 1.0)

@sim.column('parcels', 'parcel_acres')
def parcel_acres(parcels):
    return parcels.acres

@sim.column('buildings', 'year_built_1940to1950')
def year_built_1940to1950(buildings):
    return (buildings.year_built >= 1940) & (buildings.year_built < 1950)

@sim.column('buildings', 'year_built_1950to1960')
def year_built_1950to1960(buildings):
    return (buildings.year_built >= 1950) & (buildings.year_built < 1960)

@sim.column('buildings', 'year_built_1960to1970')
def year_built_1960to1970(buildings):
    return (buildings.year_built >= 1960) & (buildings.year_built < 1970)

@sim.column('buildings', 'year_built_1970to1980')
def year_built_1970to1980(buildings):
    return (buildings.year_built >= 1970) & (buildings.year_built < 1980)

@sim.column('buildings', 'year_built_1980to1990')
def year_built_1980to1990(buildings):
    return (buildings.year_built >= 1980) & (buildings.year_built < 1990)

@sim.column('buildings', 'sqft_per_unit', cache=True)
def unit_sqft(buildings):
    return (buildings.residential_sqft /
            buildings.residential_units.replace(0, 1)).fillna(0).astype('int')

@sim.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


def get_year():
    year = sim.get_injectable('year')
    if year is None:
        year = 2015
    return year


@sim.model('build_networks')
def build_networks(settings , store, parcels):
    edges, nodes = store['edges'], store['nodes']
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])

    max_distance = settings['build_networks']['max_distance']
    net.precompute(max_distance)

    #SETUP POI COMPONENTS
    on_ramp_nodes = nodes[nodes.on_ramp]
    net.init_pois(num_categories=4, max_dist=max_distance, max_pois=1)
    net.set_pois('onramps', on_ramp_nodes.x, on_ramp_nodes.y)

    parks = store.parks
    net.set_pois('parks', parks.x, parks.y)

    schools = store.schools
    net.set_pois('schools', schools.x, schools.y)

    transit = store.transit
    net.set_pois('transit', transit.x, transit.y)


    sim.add_injectable("net", net)

    p = parcels.to_frame(parcels.local_columns)

    p['node_id'] = net.get_node_ids(p['x'], p['y'])

    #p.to_csv('data/parcels.csv')
    sim.add_table("parcels", p)


@sim.model('nrh_simulate2')
def nrh_simulate2(buildings, aggregations):
    return utils.hedonic_simulate("nrh2.yaml", buildings, aggregations,
                                  "non_residential_price")


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

sim.run([#'scheduled_development_events'
         'neighborhood_vars'
         ,'rsh_simulate','nrh_simulate','nrh_simulate2'
         ,'households_transition',  "hlcm_simulate"
         ,"price_vars"
], years=range(2015,2016))

sim.get_table('nodes').to_frame().to_csv('data/nodes.csv')
sim.get_table('buildings').to_frame(['building_id','residential_price','non_residential_price','distance_to_park','distance_to_school',]).to_csv('data/buildings.csv')
sim.get_table('households').to_frame(['household_id', 'building_id', 'persons', 'age_of_head', 'income', 'income_quartile']).to_csv('data/households.csv')

#sim.get_table('parcels').to_frame().to_csv('data/parcels.csv')



