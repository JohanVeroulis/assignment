from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta
import csv
import json

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# input_csv_file = '/opt/airflow/dags/files/cvas_data_transactions.csv'
# output_json_file = '/opt/airflow/dags/files/cvas_data_transactions.json'
# csv_data =[]
# input_csv_file1 = '/opt/airflow/dags/files/subscribers.csv'
# output_json_file1 = '/opt/airflow/dags/files/subscribers.json'
# csv_data1 =[]

# #function to convert csv to json
# def convert_csvfile():
#     with open(input_csv_file, 'r') as data:
#         reader = csv.DictReader(data, delimiter=',')
#         for row in reader:
#             csv_data.append(row)
#             print(csv_data)
#             with open(output_json_file, 'a') as outfile:
#                 json.dump(row, outfile)
#                 #print(a)

# #function to convert csv to json
# def convert_csvfile1():
#     with open(input_csv_file1, 'r') as data1:
#         reader = csv.DictReader(data1, delimiter=',')
#         for row in reader:
#             #csv_data.append(row)
#             with open(output_json_file1, 'a') as outfile1:
#                 json.dump(row, outfile1)
#                 #print(a)


with DAG(dag_id="data_pipeline", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

     
    is_data_file_available = FileSensor(
            task_id="is_data_file_available",
            fs_conn_id="dataz_path",
            filepath="cvas_data_transactions.csv",
            poke_interval=5,
            timeout=20
    )

    is_data_file_available1 = FileSensor(
            task_id="is_data_file_available1",
            fs_conn_id="dataz_path",
            filepath="subscribers.csv",
            poke_interval=5,
            timeout=20
    )

    #Create a directory in hdfs and put in it transaction.csv
    saving_data = BashOperator(
        task_id="saving_data",
        bash_command="""
            hdfs dfs -mkdir -p /data && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/cvas_data_transactions.csv /data
            """
    )

    #Saving in same directory 
    saving_data2 = BashOperator(
        task_id="saving_data2",
        bash_command="""
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/subscribers.csv /data
            """
    )

    # #convert csv to json
    # convert_csvfile_tojson = PythonOperator(
    #         task_id="convert_csvfile_tojson",
    #         python_callable=convert_csvfile
    # )

    # #convert csv to json2 
    # convert_csvfile_tojson1 = PythonOperator(
    #         task_id="convert_csvfile_tojson1",
    #         python_callable=convert_csvfile1
    # )

    #Creating a hive table for cvas.json
    creating_data_table = HiveOperator(
        task_id="creating_data_table",
        hive_cli_conn_id="hive-conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS data_final(
                sub_id STRING,
                day_id STRING,
                amount FLOAT,
                channel_name STRING,
                activation_date STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )


    #Running Spark Job to process the data
    data_processing_insert = SparkSubmitOperator(
        task_id="data_processing_insert",
        conn_id="spark_conn",
        application="/opt/airflow/dags/scripts/data_processing.py",
        verbose=False
    )


is_data_file_available >> is_data_file_available1 >> saving_data >> saving_data2 >> creating_data_table

    


   
     

 
     




