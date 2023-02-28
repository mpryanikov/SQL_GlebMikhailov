# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

import pandas as pd
import numpy as np

# ## Подключение к бд и заливка данных

# <a href="https://pythonru.com/biblioteki/ustanovka-i-podklyuchenie-sqlalchemy-k-baze-dannyh">Установка и подключение SQLAlchemy к базе данных: mysql, postgresql, sqlite3 и oracle</a>

# + active=""
# pip install sqlalchemy
# -

import sqlalchemy

sqlalchemy.__version__

# + active=""
# !pip install pyodbc
# -

import pyodbc

import warnings
warnings.filterwarnings('ignore')

conn = pyodbc.connect('DSN=TestDB;Trusted_Connection=yes;')


def select(sql):
  return pd.read_sql(sql,conn)


cur = conn.cursor()
sql = '''
drop table if exists Employee
create table Employee(Id int, Salary int)
insert into Employee(Id, Salary) values (1, 100)
insert into Employee(Id, Salary) values (2, 200)
insert into Employee(Id, Salary) values (3, 300)
'''
cur.execute(sql)
conn.commit()
cur.close()
sql = '''select * from Employee t'''
select(sql)

# ### Создание, подключение и заливка данных

df = pd.read_csv('../data/german_credit_augmented.csv')
# df

df['contract_dt'] = pd.to_datetime(df['contract_dt'],format='%Y-%m-%d %H:%M:%S')

df.dtypes

# + active=""
# # не работает
# df.fillna(sqlalchemy.sql.null(), inplace=True)
# -

df = df.replace({np.nan:None})
# df

# <a href="https://vc.ru/dev/245799-chto-vybrat-text-ili-varchar-max">
# Что выбрать, text или varchar (MAX)?</a>

# <a href="https://learn.microsoft.com/ru-ru/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=azuresqldb-current">
# Вставка кадра данных Python в таблицу SQL</a><br>
# <a href="https://learn.microsoft.com/ru-ru/sql/machine-learning/python/python-libraries-and-data-types?source=recommendations&view=sql-server-ver16">
# Сопоставления типов данных между Python и SQL Server</a>

# +
cur = conn.cursor()
sql = '''
drop table if exists german_credit;
CREATE TABLE german_credit (
    age              INTEGER,
    sex              VARCHAR(max),
    job              INTEGER,
    housing          VARCHAR(max),
    saving_accounts  VARCHAR(max),
    checking_account VARCHAR(max),
    credit_amount    INTEGER,
    duration         INTEGER,
    purpose          VARCHAR(max),
    [default]        INTEGER,
    contract_dt      DATETIME,
    client_id        INTEGER
);
'''
cur.execute(sql)
conn.commit()

for index,row in df.head(1000).iterrows():
    cur.execute('''INSERT INTO german_credit(
                    [age],[sex],[job],[housing],[saving_accounts],
                    [checking_account],[credit_amount],[duration],[purpose],[default],
                    [contract_dt],[client_id]) 
                    values (?,?,?,?,?,?,?,?,?,?,?,?)
    ''', 
                    row['age'], 
                    row['sex'], 
                    row['job'],
                    row['housing'],                    
                    row['saving_accounts'],
                    row['checking_account'],
                    row['credit_amount'],
                    row['duration'],
                    row['purpose'],
                    row['default'],
                    row['contract_dt'],
                    row['client_id'])
    
conn.commit()
cur.close()
sql = '''select * from german_credit t'''
select(sql)

# +
transactions = pd.read_csv('../data/german_credit_augmented_transactions.csv')
transactions['dt'] = pd.to_datetime(transactions['dt'],format='%Y-%m-%d %H:%M:%S')
transactions = transactions.replace({np.nan:None})

cur = conn.cursor()
sql = '''
drop table if exists client_transactions;
CREATE TABLE client_transactions (
    dt               datetime,
    client_id        int,
    amount           decimal(19,4)
);
'''
cur.execute(sql)
conn.commit()

for index,row in transactions.iterrows():
    cur.execute('''INSERT INTO client_transactions(
                    [dt],[client_id],[amount]
                    ) 
                    values (?,?,?)
    ''', 
                    row['dt'],
                    row['client_id'], 
                    row['amount']
               )
    
conn.commit()
cur.close()
sql = '''select * from client_transactions t'''
select(sql)
# -

conn.close()


