# data_engineering
This is a data engineering project that I completed for myself to get hands on spark, airflow and ETL.

Basically, it consists of two docker-compose apps: Airflow and Spark.
There is a pipeline scheduled that has two branches. One extracts raw datasets from S3 to local PostgreSQL, transforms and loads data from staging tables to dimensional tables. In the end, data quality checks are run.
Another one loads data from S3 (JSON logs on user activity), processes the data into analytics tables using Spark, and loads them back into S3.

So, this repository could be an example of my skills in creating ETL pipelines. Also, feel free to fork it and/or use it for educational purposes.
