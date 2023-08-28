# coding=utf-8

### IMPORTS ###

# time-related work
from datetime import datetime, timedelta

# data manipulation
import numpy as np # to work with NaN`s
import pandas as pd
import pandahouse as ph # Connect with clickhouse DB

# dag-related
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

### PARAMS AND CONSTANTS ###

# set an offset - how many days ago to look at
# set by default to 1 (yesterday)
day_offset = 1

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'd-shiriaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 18),
}

# Интервал запуска DAG
schedule_interval = '00 16 * * *'

# set the connection with the databases
connection_source = {
    'host': 'https://clickhouse.lab.karpov.courses',
      'database':'simulator_20230720',
      'user':'student', 
      'password':'dpo_python_2020'
     }

connection_target = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw', 
    'password':'656e2b0c9c'
}

# list with the columns - needed after we do the join        
sorted_cols = [
    'event_date',
    'user_id',
    'os',
     'gender',
     'age',
     'views',
     'likes',
     'messages_received',
     'messages_sent',
     'users_received',
     'users_sent'                     
]
    
# dict with agg functions - we need it for our aggregations
aggregation_dict = {
        'likes': 'sum',
        'views': 'sum',
        'messages_sent': 'sum',
        'messages_received': 'sum',
        'users_received': 'sum',
        'users_sent': 'sum'
}

# query for feed table
query_feed = f"""
SELECT
    toDate(time) as event_date,
    toString(user_id) AS user_id,    
    os,
    toString(gender) AS gender,
    multiIf(age <= 25, '0-25', age > 55, '55+', '26-55') AS age,
    countIf(action = 'view') AS views,
    countIf(action = 'like') AS likes
    
FROM simulator_20230720.feed_actions
WHERE toDate(time) = today() - {day_offset}
GROUP BY 
    event_date, 
    user_id, 
    os, 
    gender,
    age
            """

# query for message table
query_message = f"""

WITH cte_receivers AS (
SELECT
    toDate(time) AS day,
    toString(reciever_id) as receiver_id, 
    count(DISTINCT user_id) AS users_received,
    count(user_id) AS messages_received
FROM simulator_20230720.message_actions 
WHERE toDate(time) = today() - {day_offset}
GROUP BY 
    day, 
    reciever_id
),

cte_senders AS (
SELECT
    toDate(time) AS day,
    toString(user_id) AS sender_id, 
    os, 
    toString(gender) as gender,
    multiIf(age <= 25, '0-25', age > 55, '55+', '26-55') AS age,
    count(DISTINCT reciever_id) AS users_sent,
    count(reciever_id) AS messages_sent
FROM simulator_20230720.message_actions 
WHERE toDate(time) = today() - {day_offset}
GROUP BY 
    day, 
    sender_id,
    os, 
    gender,
    age
)

SELECT 
    if(cte_senders.day != '1970-01-01', cte_senders.day, cte_receivers.day) AS event_date,
    if(sender_id != '', sender_id, receiver_id) AS user_id,
    os, 
    gender,
    age,
    messages_received,
    messages_sent,
    users_received,
    users_sent
FROM 
cte_senders 
FULL JOIN 
cte_receivers
ON cte_senders.sender_id = cte_receivers.receiver_id
"""

# query to create a target table
table_name = 'test.d_shiriaev_etl'
query_to_create = f'''
            CREATE TABLE IF NOT EXISTS {table_name}
                (
                event_date Date,
                dimension varchar(50),
                dimension_value varchar(50),
                views Float64,
                likes Float64,
                messages_received Float64,
                messages_sent Float64,
                users_received Float64,
                users_sent Float64
                ) ENGINE = MergeTree()
            ORDER BY event_date
        '''\
.replace('Float64', 'UInt64')

### DAG CREATION ###

# create a dag
@dag(default_args=default_args, 
     schedule_interval=schedule_interval, 
     catchup=False
    )
def dag_sim_shiriaev_etl_clean():  
    
    ### TASKS DEFINE ###
    
    # get cubes to work with
    @task()
    def extract(
        query, 
        connection, 
        fill_na=True
    ):

        # import a table to a dataframe
        df_cube = ph.read_clickhouse(
            query=query,
            connection=connection
        )

        # replace empty strings by NaN`s 
        # since clickhouse adds empty strings
        # instead of NaN
        if fill_na:
            df_cube.replace('', np.nan, inplace=True)

        return df_cube
    
    
    # join the cubes
    @task()
    def join_cubes(
        df_cube_1, 
        df_cube_2,
        sorted_cols,
    ):

        # do the outer join to preserve records of both tables
        df_joined = pd.merge(df_cube_1, df_cube_2, on='user_id', how='outer')

        # merge duplicate columns to replace occuring NaN`s
        for col in df_cube_1.columns:
            if col + '_x' in df_joined.columns:
                df_joined[col] = df_joined[col + '_x'].combine_first(df_joined[col + '_y'])
                df_joined.drop([col + '_x', col + '_y'], axis=1, inplace=True)

        # sort the columns        
        df_joined = df_joined.loc[:, sorted_cols]

        # replace NaN`s in views, likes, etc. by zeros
        # and convert dtypes back to uint64
        cols_to_fix = df_joined.columns[5:]

        values = {col: 0 for col in cols_to_fix}
        df_joined.fillna(value=values, inplace=True)

        dtypes = {col: 'uint64' for col in cols_to_fix}
        df_joined = df_joined.astype(dtype=dtypes)

        return df_joined
    
    
    # do an aggregation by a column provided
    @task()
    def aggregate_by(
        df, 
        group_column,
        aggregation_dict,
    ):   

        # aggregate
        grouped = df.groupby(['event_date', group_column], dropna=False).agg(aggregation_dict).reset_index()

        # rename and make dimension column
        grouped.rename(columns={group_column: 'dimension_value'}, inplace=True)
        grouped.insert(1, 'dimension', group_column)

        return grouped
    
    
    # concat the aggregated dataframes
    @task()
    def concatenate_cubes(*dfs, replace_na=True):

        combined_df = pd.concat(dfs)

        # drop the index to make it more clear
        combined_df.reset_index(drop=True, inplace=True)
        
        if replace_na:
            combined_df.fillna(value='missing_value', inplace=True)

        return combined_df
    

    # load to the target clickhouse schema
    @task()
    def load(
        df,
        query_to_create,
        connection_target,
    ):

        ph.execute(query_to_create, connection=connection_target) 
        ph.to_clickhouse(df, 'd_shiriaev_etl', index=False, connection=connection_target)
        print('DONE')
        
    ### DEFINE THE DAG STRUCTURE ###    

    # get the data
    df_feed = extract(query_feed, connection_source)
    df_message = extract(query_message, connection_source)
    
    # do the join
    df_joined = join_cubes(df_feed, df_message, sorted_cols=sorted_cols)
    
    # do a series of aggregation
    dfs_aggregated = [aggregate_by(df_joined, group_column, aggregation_dict) for group_column in ['os', 'gender', 'age']]  

    # merge aggregated dfs
    df_concatenated = concatenate_cubes(*dfs_aggregated)   
    
    # load the merged df
    load(
        df_concatenated, 
        query_to_create,
        connection_target,
    )

# launch the dag
dag_sim_shiriaev_etl_clean = dag_sim_shiriaev_etl_clean()
