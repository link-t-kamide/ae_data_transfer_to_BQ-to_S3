"""
"""

import pandas as pd
import sqlalchemy as sa
import time
import datetime as dt
import math

import os
from os.path import join, dirname
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import boto3

print('exec test')
dotenv_path = join(dirname(__file__), '2.env')
load_dotenv(dotenv_path)

HOST = 'stg-ae-cmplus-db.cluster-cewuy8brfcku.ap-northeast-1.rds.amazonaws.com'
PORT = os.environ["PORT"]
DB_USERNAME = os.environ["DB_USERNAME"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
DB_TABLE_NAME =  os.environ["TABLE_NAME"] #移行元DBの対象テーブル名

BATCH_SIZE = 100000 #DBから一度に取ってくるレコードの行数。あまり大きすぎるとメモリに乗らなくなりそう
BQ_PROJECT_NAME = os.environ["BQ_PROJECT_NAME"]
BQ_DATASET_NAME = os.environ["BQ_DATASET_NAME"]
BQ_TABLE_NAME = os.environ["TABLE_NAME"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './client_credentials.json'

url = f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{HOST}:{PORT}/{DB_NAME}?charset=utf8'
engine = sa.create_engine(url, echo=False)
process_start_time = time.time()

# pandasでSQL実行+実行結果を取得
TABLE_NAME = "advice_logs"
TABLE_NAME1 = "achievement_list_logs"
# pandasでSQL実行+実行結果を取得
df = pd.read_sql('select * from advice_logs where DATE(created_at) = CURRENT_DATE()-INTERVAL 15 DAY limit 5000', url)
df1 = pd.read_sql('select * from achievement_list_logs where DATE(created_at) = CURRENT_DATE()-INTERVAL 15 DAY limit 5000', url)
# データ型変形
df['advice_loggable_id'] = df['advice_loggable_id'].astype(str)
df['score'] = df['score'].astype(str)
# parquet 保存
now = dt.datetime.now()
time = now.strftime('%Y%m%d')
file_title = f'{time}_{TABLE_NAME}'
file_title_1 = f'{TABLE_NAME}'
file_title1 = f'{time}_{TABLE_NAME1}'
file_title1_1 = f'{TABLE_NAME1}'

df.to_parquet('result/{}.parquet'.format(file_title))
df.to_parquet('result/{}.parquet'.format(file_title_1))
df.to_csv('result/{}.csv'.format(file_title), index=False, encoding='utf_8_sig')

df1.to_parquet('result/{}.parquet'.format(file_title1))
df1.to_parquet('result/{}.parquet'.format(file_title1_1))

s3_client = boto3.client('s3')

Filename = 'result/{}.parquet'.format(file_title)
Bucket = 'sample-kamide-prod'
Key = 'bigquery/advice_logs/{}.parquet'.format(file_title)
s3_client.upload_file(Filename, Bucket, Key)

Filename = 'result/{}.parquet'.format(file_title_1)
Bucket = 'sample-kamide-prod'
Key = 'bigquery/advice_logs/{}.parquet'.format(file_title_1)
s3_client.upload_file(Filename, Bucket, Key)

Filename = 'result/{}.csv'.format(file_title)
Bucket = 'sample-kamide-prod'
Key = 'bigquery/advice_logs/{}.csv'.format(file_title)
s3_client.upload_file(Filename, Bucket, Key)

Filename = 'result/{}.parquet'.format(file_title1)
Bucket = 'sample-kamide-prod'
Key = 'bigquery/achievement_list_logs/{}.parquet'.format(file_title1)
s3_client.upload_file(Filename, Bucket, Key)

Filename = 'result/{}.parquet'.format(file_title1_1)
Bucket = 'sample-kamide-prod'
Key = 'bigquery/achievement_list_logs/{}.parquet'.format(file_title1_1)
s3_client.upload_file(Filename, Bucket, Key)
