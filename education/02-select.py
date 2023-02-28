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

# #### Подключение к бд и заливка данных

import sqlalchemy
import pyodbc
import warnings
warnings.filterwarnings('ignore')

conn = pyodbc.connect('DSN=TestDB;Trusted_Connection=yes;')


def select(sql):
  return pd.read_sql(sql,conn)


sql = '''select * from german_credit t'''
select(sql)

# # 3. Select

# ## 1. Псевдонимы

sql = '''SELECT t.age * 3 AS age_mult3,
       t.housing
FROM german_credit AS t
'''

select(sql)

# ## 2. Базовые операции со столбцами

sql = '''select t.*, 
t.age * 3 as age_mult3,
t.age + t.credit_amount as age_plus_amount,
t.age * 1.0 / t.credit_amount as age_div_amount,
t.age as age_2
from german_credit t
'''

select(sql)

# ## 3. Where

sql = '''select count(1) from german_credit t 
where t.contract_dt between 
Convert(Date, '01.01.2007', 104) and Convert(Date, '31.12.2007', 104)
'''

select(sql)

sql = '''select * from german_credit t 
where t.contract_dt between Convert(Date, '01.01.2007', 104) and Convert(Date, '31.12.2007', 104)
and t.purpose in ('car' ,'repairs')
order by t.contract_dt desc, credit_amount 
'''

select(sql)

# ## 5. Case when

# ### Доля клиентов с размером кредита > 1000:

sql = '''select count(*) from german_credit t'''
select(sql)

sql = '''select count(*) from german_credit t
where t.credit_amount > 1000
'''

select(sql)

884/1000

sql = '''select t.credit_amount,
case when t.credit_amount > 1000 then 1 else 0 end as greater_1000_flag,
iif(t.credit_amount > 1000,1,0) as greater_1000_flag2
from german_credit t
'''

select(sql)

sql = '''select 
avg(case when t.credit_amount > 1000 then 1.0 else 0 end) as greater_1000_frac
from german_credit t
'''

select(sql)

# ## 7. Создание таблицы

cur = conn.cursor()
sql = '''
drop table if exists greater_1000_credit;

select * 
into greater_1000_credit
from german_credit t
where t.credit_amount > 1000
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''select * from greater_1000_credit t'''

select(sql)

# ----------------

conn.close()


