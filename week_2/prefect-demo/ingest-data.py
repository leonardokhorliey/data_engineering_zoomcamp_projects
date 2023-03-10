from datetime import timedelta
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash


# df = pd.read_csv('green_tripdata_2019-01.csv.gz')
# len(df)

# zone_df = pd.read_csv('taxi+_zone_lookup.csv')
# len(zone_df)



# script = pd.io.sql.get_schema(df, name='green_trip_data', con=engine)
# create_table_script = script.replace('CREATE', 'DROP TABLE IF EXISTS green_trip_data;\n CREATE')

# print(create_table_script)

# ##Get zones schema
# zone_script = pd.io.sql.get_schema(zone_df, name='taxi_zones', con=engine)
# zone_script = zone_script.replace('CREATE', 'DROP TABLE IF EXISTS taxi_zones; CREATE')
# print(zone_script)

# ## create tables
# with engine.connect() as conn:

#     conn.execute(create_table_script)
#     conn.execute(zone_script)


# df_iter = pd.read_csv('green_tripdata_2019-01.csv.gz', iterator=True, chunksize=100000)

# while True: 
#     try: 

#         t_start = time()

#         df = next(df_iter)

#         df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
#         df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
#         df.to_sql(name='green_trip_data', con=engine, if_exists='append', index=False)

#         t_end = time()

#         print('inserted another chunk, took %.3f second' % (t_end - t_start))
#     except StopIteration:
#         print('Iteration Ended')
#         break


# ##insert zones data
# zone_df.to_sql(name='taxi_zones', con=engine, if_exists='replace', index=False)

def setup_engine(host, user, password, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    return engine

@task(name="Read CSV", log_prints=True)
def reading_csv(csv_url: str, as_iter:bool = False) -> pd.DataFrame:
    data_file: str = 'output.csv'
    if(csv_url.endswith('.csv.gz')): data_file += '.gz'

    os.system(f"wget {csv_url} -O {data_file}")
    print(data_file)
    data = pd.read_csv(data_file, compression='gzip' if csv_url.endswith('.csv.gz') else 'infer', iterator=as_iter, chunksize=100000 if as_iter else None, on_bad_lines='skip')

    return data
    

# @task(name="Create Table", log_prints=True)
def create_data_table(engine, data, db_table_name):

    try: 
        script = pd.io.sql.get_schema(data, name=db_table_name, con=engine)
        create_table_script = script.replace('CREATE', f'DROP TABLE IF EXISTS {db_table_name};\n CREATE')

        print(create_table_script)

        with engine.connect() as conn:

            conn.execute(create_table_script)

        return True
    except:
        return False


@task(name="Load to DB", log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_to_db(engine, data, db_table_name, large_file=False):

    create_data_table(engine, data, db_table_name)
    
    if (large_file):

        while True: 
            try: 

                t_start = time()

                df = next(data)

                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                
                df.to_sql(name=db_table_name, con=engine, if_exists='append', index=False)

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))
            except:
                print('Iteration Ended')
                break

    else:
        data.to_sql(name=db_table_name, con=engine, if_exists='append', index=False)



    



@flow(name="Ingest Flow")
def main():
    host='localhost'
    user='root'
    password='root'
    port='5018'
    db='ny_taxi'
    csv_urls=['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz', 
    'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv']
    table_names=['green_taxi', 'zones']

    engine = setup_engine(host, user, password, port, db)

    

    for i in range(len(csv_urls)):
        print(csv_urls[i])
        data = reading_csv(csv_urls[i], True)

        print(data)

        load_to_db(engine, data, table_names[i], True)


main()
