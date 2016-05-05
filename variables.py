import pandas as pd
import urbansim.sim.simulation as sim
from urbansim.utils import misc


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


@sim.column('buildings', 'is_office')
def is_office(buildings):
    return (buildings.building_type_id == 4).astype('int')


@sim.column('buildings', 'is_retail')
def is_retail(buildings):
    return (buildings.building_type_id == 5).astype('int')


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


@sim.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    if use == "residential": s *= 1.2
    return s


@sim.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use):
    return misc.reindex(sim.get_table('nodes')[use],
                        sim.get_table('parcels').node_id)


@sim.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    parcels = sim.get_table('parcels')
    zoning_allowed_uses = sim.get_table('zoning_allowed_uses').to_frame()

    if form == 'sf_detached':
        allowed = zoning_allowed_uses[19]
    elif form == 'sf_attached':
        allowed = zoning_allowed_uses[20]
    elif form == 'mf_residential':
        allowed = zoning_allowed_uses[21]
    elif form == 'light_industrial':
        allowed = zoning_allowed_uses[2]
    elif form == 'heavy_industrial':
        allowed = zoning_allowed_uses[3]
    elif form == 'office':
        allowed = zoning_allowed_uses[4]
    elif form == 'retail':
        allowed = zoning_allowed_uses[5]
    else:
        df = pd.DataFrame(index=parcels.index)
        df['allowed'] = True
        allowed = df.allowed

    return allowed