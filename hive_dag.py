from airflow import DAG
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
from query import create_database_query, create_database_query2, create_table_query, insert_data_query, \
      create_table_dim1_query, insert_table_dim1_query, create_table_dim2_query, insert_table_dim2_query, \
      create_table_dim3_query, insert_table_dim3_query, create_table_dim4_query, insert_table_dim4_query, \
      create_table_dim5_query, insert_table_dim5_query, create_table_fact_query, insert_table_fact_query

default_args = {'owner':'fadlil', 
                'retries':1, 
                'retry_delay':timedelta(minutes=25)}

def get_data():

    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    result = requests.get(url)
    soap = bs(result.content, 'html.parser')

    for data in soap.findAll('div', {'id':'faq2022'})[0].findAll('ul'):
        link = data.findAll('li')[1].a['href'] 
        data = requests.get(link)
        name = link.split('/')[-1]
        with open('/home/fadlil/green_taxi/parquet/'+name, 'wb')as file:
            file.write(data.content)

with DAG(dag_id = 'dag_green_taxi_2',
        default_args=default_args,
        schedule = '@daily',
        start_date = datetime(2023,5,24)
         ) as dag:
    
    # get_data_task = PythonOperator(task_id='get_data',
    #                        python_callable=get_data)
    
    create_database = HiveOperator(
        task_id='create_database_staging',
        hql = create_database_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_database2 = HiveOperator(
        task_id='create_database_dwh',
        hql = create_database_query2,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table = HiveOperator(
        task_id='create_table',
        hql=create_table_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data = HiveOperator(
        task_id='insert_data',
        hql=insert_data_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    transformation_task = BashOperator(task_id = 'spark_transformation', 
                                bash_command = '/home/fadlil/spark/bin/spark-submit --master local /home/fadlil/spark_taxi/spark1.py')

    create_table2 = HiveOperator(
        task_id='create_table_dim1',
        hql=create_table_dim1_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data2 = HiveOperator(
        task_id='insert_data_dim1',
        hql=insert_table_dim1_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table3 = HiveOperator(
        task_id='create_table_dim2',
        hql=create_table_dim2_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data3 = HiveOperator(
        task_id='insert_data_dim2',
        hql=insert_table_dim2_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table4 = HiveOperator(
        task_id='create_table_dim3',
        hql=create_table_dim3_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data4 = HiveOperator(
        task_id='insert_data_dim3',
        hql=insert_table_dim3_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table5 = HiveOperator(
        task_id='create_table_dim4',
        hql=create_table_dim4_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data5 = HiveOperator(
        task_id='insert_data_dim4',
        hql=insert_table_dim4_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table6 = HiveOperator(
        task_id='create_table_dim5',
        hql=create_table_dim5_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data6 = HiveOperator(
        task_id='insert_data_dim5',
        hql=insert_table_dim5_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    create_table7 = HiveOperator(
        task_id='create_table_fact',
        hql=create_table_fact_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    insert_data7 = HiveOperator(
        task_id='insert_data_fact',
        hql=insert_table_fact_query,
        hiveconf_jinja_translate=True,
        hive_cli_conn_id = 'hiveserver2_default'
    )

    # delete_data_task = BashOperator(task_id = 'delete_local_data', 
    #                             bash_command = 'rm -r /home/fadlil/green_taxi/parquet/*')


# get_data_task >> 
[create_database, create_database2] >> create_table >> insert_data >> transformation_task
transformation_task >> create_table2 >> insert_data2
transformation_task >> create_table3 >> insert_data3 
transformation_task >> create_table4 >> insert_data4 
transformation_task >> create_table5 >> insert_data5 
transformation_task >> create_table6 >> insert_data6 
transformation_task >> create_table7 >> insert_data7 