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


sql = '''drop table if exists Employee
create table Employee(Id int, Salary int)
insert into Employee(Id, Salary) values (1, 100)
insert into Employee(Id, Salary) values (2, 200)
insert into Employee(Id, Salary) values (3, 300)
'''

# поставим ; после каждой строки:

sql = ';\n'.join(sql.split('\n'))
print(sql)

# # 8. Заключение

# ## 2. Где тренироваться

# https://sql-ex.ru/

# https://leetcode.com/ - платный (дорогой)

# <a href="https://stepik.org/course/63054/promo">
#    Интерактивный тренажер по SQL · Stepik</a>


