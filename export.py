
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes'
edges_sql = 'SELECT from_node as [from], to_node as [to], distance as [weight] FROM urbansim.edges'
parcels_sql = 'SELECT parcel_id, luz_id, parcel_acres as acres, zoning_id as zone_id, centroid.STX as x, centroid.STY as y, distance_to_coast, distance_to_freeway FROM urbansim.parcels'
buildings_sql = 'SELECT building_id, parcel_id, development_type_id as building_type_id, COALESCE(residential_units, 0) as residential_units, residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, 0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings'
households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households'
jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'
scheduled_development_events_sql = """SELECT
                                         scheduled_development_event_id, parcel_id, development_type_id as building_type_id
                                         ,year_built, sqft_per_unit, residential_units, non_residential_sqft
                                         ,improvement_value, res_price_per_sqft, nonres_rent_per_sqft as non_residential_rent_per_sqft
                                         ,COALESCE(stories,1) as stories FROM urbansim.scheduled_development_event"""
schools_sql = """SELECT objectID as id, Shape.STX as x ,Shape.STY as y FROM gis.schools WHERE SOCType IN ('Junior High Schools (Public)','K-12 Schools (Public)','Preschool','Elemen Schools In 1 School Dist. (Public)','Elementary Schools (Public)','Intermediate/Middle Schools (Public)','High Schools (Public)','Private')"""
parks_sql = """SELECT subparcel as park_id, shape.STCentroid().STX x, shape.STCentroid().STY y FROM gis.landcore WHERE lu IN (7207,7210,7211,7600,7601,7604,7605)"""
transit_sql = 'SELECT x, y, stopnum FROM gis.transit_stops'
household_controls_sql = """SELECT yr as [year], hh_income_id as income_quartile, hh FROM isam.defm.households WHERE dem_version = 'S0021' and eco_version = '001' AND yr >= 2015"""
employment_controls_sql = """SELECT yr as [year], jobs as number_of_jobs, sector_id FROM isam.defm.jobs WHERE dem_version = 'S0021' and eco_version = '001' AND yr >= 2015"""


nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine, index_col='parcel_id')
buildings_df = pd.read_sql(buildings_sql, urbansim_engine, index_col='building_id')
households_df = pd.read_sql(households_sql, urbansim_engine, index_col='household_id')
jobs_df = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
building_sqft_per_job_df = pd.read_sql(building_sqft_per_job_sql, urbansim_engine)
scheduled_development_events_df = pd.read_sql(scheduled_development_events_sql, urbansim_engine, index_col='scheduled_development_event_id')
schools_df = pd.read_sql(schools_sql, urbansim_engine, index_col='id')
parks_df = pd.read_sql(parks_sql, urbansim_engine, index_col='park_id')
transit_df = pd.read_sql(transit_sql, urbansim_engine)
household_controls_df = pd.read_sql(household_controls_sql, urbansim_engine, index_col='year')
employment_controls_df = pd.read_sql(employment_controls_sql, urbansim_engine, index_col='year')


building_sqft_per_job_df.sort_values(['luz_id', 'development_type_id'], inplace=True)
building_sqft_per_job_df.set_index(['luz_id', 'development_type_id'], inplace=True)

edges_df.sort_values(['from', 'to'], inplace=True)
#edges_df.set_index(['from', 'to'], inplace=True)

with pd.HDFStore('data/urbansim.h5', mode='w') as store:
    store.put('nodes', nodes_df, format='t')
    store.put('edges', edges_df, format='t')
    store.put('parcels', parcels_df, format='t')
    store.put('buildings', buildings_df, format='t')
    store.put('households', households_df, format='t')
    store.put('jobs', jobs_df, format='t')
    store.put('building_sqft_per_job', building_sqft_per_job_df, format='t')
    store.put('scheduled_development_events', scheduled_development_events_df, format='t')
    store.put('schools', schools_df, format='t')
    store.put('parks', parks_df, format='t')
    store.put('transit', transit_df, format='t')
    store.put('household_controls', household_controls_df, format='t')
    store.put('employment_controls', employment_controls_df, format='t')
