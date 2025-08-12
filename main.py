from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import json

from google.cloud import bigquery
from pandas_gbq import to_gbq
import functions_framework



conn_params = {
    "host": "wenm-vms01.ad.dfoundry.co",
    "port": "5432",
    "database": "scada",
    "user": "postgres",
    "password": "1Zg*Yh69Ebmq"
}
conn_str = (
    f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}"
    f"@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
)

#Get tables
def get_partition_tables(from_time, to_time,engine):
    Query = f'''
    SELECT distinct pname FROM 
    (
        SELECT pname 
        FROM sqlth_partitions 
        WHERE (start_time BETWEEN {from_time} AND {to_time})  
        OR end_time BETWEEN {from_time} AND {to_time} 
        OR ( start_time < {to_time} AND end_time > {from_time}) 
        GROUP BY pname
    ) foo;
    '''
    # partitions = list(pd.read_sql_query(Query, engine).pname)
    return list(pd.read_sql_query(Query, engine).pname)
def get_tag_list(engine):
    Query = f'''
    select 
    json_agg(fii) from(
    SELECT 
        tagpath, CASE WHEN datatype = 0 THEN 'intvalue' WHEN datatype = 1 THEN 'floatvalue' END AS type
    FROM sqlth_te
    WHERE retired IS NULL
      AND split_part(tagpath, '/', 4) IN (
        'warning_accumulator_a_overtemperature'
      )
      ) fii;
    
    '''
    with engine.connect() as conn:
        result = conn.execute(text(Query))
        tags_list = result.scalar()
    return tags_list
def build_query_ignition(partitions, tags_list, from_time, to_time, interval):
    queries = []
    for tag in tags_list:
        for parition in partitions:
            queries.append(
                f"""
                
            SELECT
                tagpath,
                time_bucket('{interval}', TO_TIMESTAMP(t_stamp / 1000)) as interval_time,
                max({tag["type"]}) as value
            FROM
                sqlth_te LEFT JOIN {parition} ON sqlth_te.id = {parition}.tagid 
            WHERE tagpath = '{tag["tagpath"]}' 
            AND t_stamp BETWEEN  {from_time}  AND {to_time} 
            GROUP BY tagpath,interval_time
            """
            )
    full_query = "select tagpath, interval_time, max(value) as value from ( \n" + " UNION ".join(queries) + "\n ) foo group by 1,2;"

    return full_query
def cache_history_record(days,range_of_days,engine):
    tags_list = get_tag_list(engine)
    to_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()*1000 - days*24*60*60*1000
    from_time = to_time - 24*60*60*1000*range_of_days
    partitions = get_partition_tables(from_time, to_time,engine)
    print(partitions)
    print('from',datetime.fromtimestamp(from_time / 1000, tz=timezone.utc), 'to',datetime.fromtimestamp(to_time / 1000, tz=timezone.utc))
    
    Query = build_query_ignition(partitions, tags_list, from_time, to_time, '1 HOUR')
    
    data = pd.read_sql_query(Query, engine)
    data[["Site", "Line", "Tool", "Parameter"]] = data["tagpath"].str.split("/", expand=True)
    
    # Set your BigQuery project ID and dataset ID
    project_id = 'df-data-and-ai'
    dataset_id = 'staging'  # your dataset ID
    table_id = 'facility_data_per_hour'  # your table ID
        
    # Define the full table name
    table_full_name = f'{project_id}.{dataset_id}.{table_id}'
        
    # Upload the DataFrame to BigQuery (append mode)
    to_gbq(data, table_full_name, project_id=project_id, if_exists='replace')
    
    merge_sql = f"""
        MERGE `{project_id}.Scada.{table_id}` T
        USING `{project_id}.staging.{table_id}` S
        ON T.tagpath = S.tagpath and T.interval_time = S.interval_time
        WHEN MATCHED THEN
          UPDATE SET
            {', '.join([f"T.{col} = S.{col}" for col in data.columns if col != 'id'])}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(data.columns)})
          VALUES ({', '.join([f"S.{col}" for col in data.columns])})
        """
    client = bigquery.Client()
    client.query_and_wait(merge_sql)
    return     ("from "
    + datetime.fromtimestamp(from_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    + " to "
    + datetime.fromtimestamp(to_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    + " cached")

@functions_framework.http
def main_call(request): # It have to have a request
    engine = create_engine(conn_str)  # this has to be called inside function
    try:
        message = cache_history_record(0, 3,engine)
        return message
    except Exception as e:
        return f'An error occurred: {str(e)}', 500

