#!/usr/bin/env python
# coding: utf-8

# We want to load the  data('./data/yellow_tripdata_2021-01.parquet')to the  postgres database .
# We  use a python library for pandas : SQLAlchemy: https://www.sqlalchemy.org/ .
# 
# # pip install sqlalchemy

# We're going to create a database connection with the following credentials:
#  user: postgres,
#  password: postgres ,
#  database: postresdb.
#  We'll call this connection 'engine'
#  NB: ensure  to install the posgresql driver  psycopg2-binary
# engine = create_engine('postgresql://username:password@ip:port/dbname')

import pandas as pd
from sqlalchemy import create_engine

username = 'postgres'
password = 'postgres'
dbname = 'postgresdb'
host = 'localhost'
port = '5432'
table_name = 'yellow_taxi'

data = './data/yellow_tripdata_2021-01.parquet'
df = pd.read_parquet(data, engine='pyarrow')

def main():
    batch = 10000
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
    
    # Create the table with no data yet
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')  # Fixed typo here
    
    # Load the data into the table in batches
    for i in range(0, len(df), batch):
        batch_data = df.iloc[i:i + batch]  # Fixed `data` to `df` for proper slicing
        batch_data.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        print(f"Batch {i // batch + 1} successfully inserted")

if __name__ == '__main__':
    main()
