#Data Engineering Technical Assessment.

The objective of this assignment is to implement a distributed system that handles csv data, transforms those and persists them in parquet format. Additionally records need to be persisted in a NoSQL database, preferably HBase. Cassandra or any ither setup is also welcome. The system should be composed of the following three modules:
1. A distributed file system (Hadoop cluster) where the csv dataset-files will be stored and the resulting parquet files will be persisted.
2. A spark cluster that will run on top of Hadoop and will process the csv data in order to produce the parquet files.
3. An HBase cluster (or other NoSQL technology) where subscribers records will be persisted.

#GETTING STARTED
The project uses Apache Airflow, Hadoop, Hive, Spark technologies. Also in this project I use a Docker container that consists all that tools where used for that project.
The container is set up to connect with hadoop, hive and pyspark.

* In directory airflow-section-3 built your container  

* Built your container:
docker build -t airflow-basic .

* Run your container
docker run --rm -d -p 8080:8080 airflow-basic

* Show running docker containers
docker ps

* find the container_id airflow-section-3-airflow and get in with this command
docker exec -it container_id /bin/bash 

* In Airflow UI you can see in Dags my pipeline 

When built and run your container navigate to localhost:8080 to log in Airflow
user=airflow
password=airflow

After navigate to localhost:32762 to log in Hue 
user=root
password=root

The project consists two python files. 
The first file is data_pipeline.py which there is in dag_solution my pipeline and the second file is the data_processing.py with spark which is inside in airflow/dags/scripts and csv files there are inside the airflow/dags/files.

I prepared all connections to work. 


#Problems with last task in my pipeline 

My last task with task_id="data_processing_insert" which use SparkSubmitOperator I think that is not connecting with the container. I have try a lot but it does not work. I checked all the connections, I read very carefully the docker-compose yaml file to find solution but it does not work. 
The data_processing.py do all the prepare process that you asked me. 
Also I tried to convert csv files to json files but the last task still not work.
