import requests
import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import csv


df = ""

#using import module
def download_csv_to_ec2():
    url = 'https://opendata.ecdc.europa.eu/covid19/nationalcasedeath_eueea_daily_ei/csv/data.csv'
    r = requests.get(url)
    #with open('/home/ec2-user/data/covid.csv','wb') as f:  => this is wrong
    with open('./covid.csv','wb') as f:
        f.write(r.content)


#upload data to S3
def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name,replace=True)


#upload updated data to s3
def upload_updated_to_s3(filename: str, key: str, bucket_name: str) -> None:
    # upload csv file
    df = pd.read_csv('./covid.csv')
    df = df.drop(df.columns[[0,7,8,9]],axis=1) #axis=1 means column, axis=0 means row
    df.to_csv('./covid_updated.csv', index=False)
    # upload to s3
    hook = S3Hook('aws_default')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name, replace=True)


#download updated data from S3 to EC2
#def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
#    hook = S3Hook('aws_default')
#    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path, replace=True)
#    return file_name


#def rename_file(ti, new_name: str) -> None:
#    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
#    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
#    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")


def create_table(): #import pandas as pd
    hook = MySqlHook(mysql_conn_id='mysql_default')
    hook.run("""
    CREATE TABLE IF NOT EXISTS covid (
        day INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        cases INT NOT NULL,
        deaths INT NOT NULL,
        countries VARCHAR(20) NOT NULL,
        continent VARCHAR(10) NOT NULL
        );
    """)


#def truncate_table():
#    hook = MySqlHook(mysql_conn_id='mysql_default')
#    hook.run("TRUNCATE TABLE covid;")




def batch_csv():
    with open('./covid_updated.csv') as f:
        reader = csv.reader(f)
        batch = []
        for row in reader:
            batch.append(row)
            if len(row) == 7:
                yield batch
                del batch[:]
        yield batch


def insert_mysql():
    mysqlConnection = MySqlHook(mysql_conn_id='mysql_default')
    a = mysqlConnection.get_conn()
    c = a.cursor()
    sql ="""
    INSERT INTO covid (day, month, year, cases, deaths, countries, continent)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    batcher = batch_csv()
    for batch in batcher:
        c.executemany(sql, [row[0:7] for row in batch])

    a.commit()



#DAG File
with DAG(
    dag_id='airflow',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 3),
    catchup=False
) as dag:
    # upload csv to ec2
    task_download_csv_to_ec2 = PythonOperator(
        task_id = 'download_csv_to_ec2',
        python_callable=download_csv_to_ec2,
    )

    # upload a csv file to s3
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3, #function name above
        op_kwargs={
            'filename': './covid.csv',
            'key': 'covid_original.csv',
            'bucket_name': 'jaewon-bucket',
        }
    )

    # upload an updated file csv to s3
    task_upload_updated_to_s3 = PythonOperator(
        task_id='upload_updated_to_s3',
        python_callable=upload_updated_to_s3, #function name above
        op_kwargs={
            'filename': './covid_updated.csv',
            'key': 'covid_updated.csv',
            'bucket_name': 'jaewon-bucket',
        }
    )

    # Download a file
#    task_download_from_s3 = PythonOperator(
#        task_id='download_from_s3',
#        python_callable=download_from_s3,
#        op_kwargs={
#            'key': 'covid_updated.csv',
#            'bucket_name': 'jaewon-bucket',
#            'local_path': './'
#        }
#    )

 #   task_rename_file = PythonOperator(
 #       task_id='rename_file',
  #      python_callable=rename_file,
   #     op_kwargs={
#            'new_name': 'covid_updated.csv'
#        }
#    )


    #create_table
    task_create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )


    #insert_mysql
    task_insert_mysql = PythonOperator(
        task_id='insert_mysql',
        python_callable=insert_mysql,
    )


    task_download_csv_to_ec2 >> task_upload_to_s3 >> task_upload_updated_to_s3 >> task_create_table >> task_insert_mysql