from cassandra.cluster import Cluster
from env_variable import *
from datetime import datetime
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()


session.set_keyspace('statistics')

# session.execute('drop table if exists total_amount_statistics')
# session.execute("""
#     CREATE TABLE IF NOT EXISTS total_amount_statistics (
#         year text,
#         day text,
#         month text,
#         total_amount float,
#         timestamp timestamp,
#         partition_key int,
#         PRIMARY KEY (partition_key, year, day, month)
#     )
# """)

# print("All table created successfully")

# delete row that have year = 2020 and month >= 6
# Step 1: Select rows to delete
# Step 1: Collect rows to delete with multiple queries
months_to_check = ['6', '7', '8', '9', '10', '11', '12']
rows_to_delete = []

for month in months_to_check:
    query = f"SELECT year, month, day, partition_key FROM total_amount_statistics WHERE year = '2020' AND month = '{month}' ALLOW FILTERING"
    rows_to_delete.extend(session.execute(query))

# Step 2: Delete the fetched rows using their primary key values
for row in rows_to_delete:
    delete_query = f"DELETE FROM total_amount_statistics WHERE partition_key = {row.partition_key} AND year = '{row.year}' AND month = '{row.month}' AND day = '{row.day}'"
    session.execute(delete_query)

# Step 3: Verify the results
rows = session.execute('SELECT * FROM total_amount_statistics')
rows = sorted(rows, key=lambda x: (x.year, x.month, x.day))
for row in rows:
    print(row.year, row.month, row.day, row.total_amount, row.timestamp)

# Close the connection
cluster.shutdown()
    

# print('Data updated successfully')