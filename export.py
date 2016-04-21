
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

nodes_sql = 'SELECT node as node_id, x, y FROM urbansim.nodes'
edges_sql = 'SELECT from_node as [from], to_node as [to], distance as [weight] FROM urbansim.edges'
parcels_sql = 'SELECT parcel_id, luz_id, parcel_acres as acres, centroid.STX as x, centroid.STY as y FROM urbansim.parcels'
buildings_sql = 'SELECT building_id, parcel_id, development_type_id as building_type_id, COALESCE(residential_units, 0) as residential_units, residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, 0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built FROM urbansim.buildings'
households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households'
jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'
scheduled_development_events_sql = """SELECT
                                         scheduled_development_event_id, parcel_id, development_type_id as building_type_id
                                         ,year_built, sqft_per_unit, residential_units, non_residential_sqft
                                         ,improvement_value, res_price_per_sqft, nonres_rent_per_sqft as non_residential_rent_per_sqft FROM urbansim.scheduled_development_event"""

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine, index_col='parcel_id')
buildings_df = pd.read_sql(buildings_sql, urbansim_engine, index_col='building_id')
households_df = pd.read_sql(households_sql, urbansim_engine, index_col='household_id')
jobs_df = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
building_sqft_per_job_df = pd.read_sql(building_sqft_per_job_sql, urbansim_engine)
scheduled_development_events_df = pd.read_sql(scheduled_development_events_sql, urbansim_engine, index_col='scheduled_development_event_id')

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
