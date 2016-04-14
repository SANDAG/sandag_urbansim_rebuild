
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

nodes_sql = 'SELECT node as node_id, x, y FROM urbansim.nodes'
edges_sql = 'SELECT from_node as [from], to_node as [to], distance as [weight] FROM urbansim.edges'
parcels_sql = 'SELECT parcel_id, parcel_acres, centroid.STX as x, centroid.STY as y FROM urbansim.parcels'
buildings_sql = 'SELECT id as building_id, parcel_id FROM urbansim.buildings'
households_sql = 'SELECT household_id, building_id, income FROM urbansim.households'

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine, index_col='parcel_id')
buildings_df = pd.read_sql(buildings_sql, urbansim_engine, index_col='building_id')
households_df = pd.read_sql(households_sql, urbansim_engine, index_col='household_id')

edges_df.sort_values(['from', 'to'], inplace=True)
#edges_df.set_index(['from', 'to'], inplace=True)

with pd.HDFStore('data/urbansim.h5', mode='w') as store:
    store.put('nodes', nodes_df, format='t')
    store.put('edges', edges_df, format='t')
    store.put('parcels', parcels_df, format='t')
    store.put('buildings', buildings_df, format='t')
    store.put('households', households_df, format='t')
