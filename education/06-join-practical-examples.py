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


sql = '''select * from client_transactions t'''
select(sql)

# # 6. Джойны (Продолжение)

# ## 8. Ежемесячный отчет (практический пример)

# #### прислали транзакции по клиентам:

# + active=""
# transactions = pd.read_csv('../data/german_credit_augmented_transactions.csv')
# -

sql = '''select top(5) * from client_transactions t'''
select(sql)

sql = '''select count(*) from client_transactions t'''
select(sql)

# сгруппируем:

sql = '''select 
year(t.dt) as year,  month(t.dt) as month,
count(1) as transaction_cnt,
sum(t.amount) as amount_sum

 from client_transactions t
group by year(t.dt), month(t.dt)
order by year(t.dt), month(t.dt)
'''

select(sql)

# нет сентября...

# #### надо сгенерить заготовку, чтобы были все месяцы:

# <a href="https://stackovergo.com/ru/q/3063246/how-to-generate-a-range-of-dates-in-sql-server">
#     Как создать диапазон дат в SQL Server</a>

sql = '''
Declare   @FromDate   Date,
          @ToDate     Date
select @FromDate = min(t.dt) from client_transactions t
select @ToDate = max(t.dt) from client_transactions t;  

WITH n AS 
(
  SELECT TOP (DATEDIFF(DAY, @FromDate, @ToDate) + 1) 
    n = ROW_NUMBER() OVER (ORDER BY [object_id])
  FROM sys.all_objects
),
p as
(
SELECT DATEADD(DAY, n-1, @FromDate) as dt
FROM n
)
select year(dt) year, month(dt) month from p
'''

select(sql)

sql = '''
Declare   @FromDate   Date,
          @ToDate     Date
select @FromDate = min(t.dt) from client_transactions t
select @ToDate = max(t.dt) from client_transactions t;  

WITH n AS 
(
  SELECT TOP (DATEDIFF(DAY, @FromDate, @ToDate) + 1) 
    n = ROW_NUMBER() OVER (ORDER BY [object_id])
  FROM sys.all_objects
),
p as
(
SELECT DATEADD(DAY, n-1, @FromDate) as dt
FROM n
),
ym as(
select year(dt) year, month(dt) month from p
group by year(dt), month(dt)
),
tr as(
select 
year(t.dt) as year,  month(t.dt) as month,
count(1) as transaction_cnt,
sum(t.amount) as amount_sum
from client_transactions t
group by year(t.dt), month(t.dt)
--order by year(t.dt), month(t.dt)
)

--select * from ym

--/*
select ym.year, ym.month,
coalesce(tr.transaction_cnt,0) as transaction_cnt,
coalesce(tr.amount_sum,0) as amount_sum
from ym
left join tr on tr.year = ym.year and tr.month = ym.month
--*/
'''

select(sql)

# ## 9. Ежемесячный отчет на пользователя (практический пример)

sql = '''select * from german_credit t'''
select(sql)

sql = '''select distinct t.client_id from german_credit t'''

select(sql)

sql = '''
Declare   @FromDate   Date,
          @ToDate     Date
select @FromDate = min(t.dt) from client_transactions t
select @ToDate = max(t.dt) from client_transactions t;  

WITH n AS 
(
  SELECT TOP (DATEDIFF(DAY, @FromDate, @ToDate) + 1) 
    n = ROW_NUMBER() OVER (ORDER BY [object_id])
  FROM sys.all_objects
),
p as
(
SELECT DATEADD(DAY, n-1, @FromDate) as dt
FROM n
),

--список дат
dates as(
select year(dt) year, month(dt) month from p
group by year(dt), month(dt)
),

--клиенты
clients as (
select distinct t.client_id from german_credit t
),

--привязка каждого клиента к дате
clients_month as
(SELECT t.year, t.month, c.client_id FROM dates t
join clients c on 1=1),

--реестр транзакций (из файла)
trans_month as(
select 
year(t.dt) as year,  month(t.dt) as month,
t.client_id,
count(1) as transaction_cnt,
sum(t.amount) as amount_sum
from client_transactions t
group by year(t.dt), month(t.dt), t.client_id
)

--/*
,client_trans_month as (

select t.client_id, t.year, t.month,
tm.transaction_cnt,
tm.amount_sum,
1 as [user],
case when tm.transaction_cnt > 0 then 1 else 0 end as active
from clients_month t
left join trans_month tm on t.client_id = tm.client_id
    and t.year = tm.year and t.month = tm.month
)
--*/

/*
select * from client_trans_month
where client_id=900
order by client_id, year, month
--*/
--/*
select t.year, t.month, sum(t.[user]) as user_cnt, sum(t.amount_sum) as amount_sum , 
sum(t.active) as active_cnt from client_trans_month t
group by t.year, t.month
order by t.year, t.month
--*/
'''

select(sql)

# #### проверим:

t = select(sql)

t['amount_sum'].sum()

sql = '''select sum(t.amount) from client_transactions t'''

select(sql)

# ## 11. Джойн таблицы самой на себя (нарастающий итог)

t = pd.DataFrame({'dt':pd.to_datetime(['2021-04-01','2021-04-02','2021-04-03'],format='%Y-%m-%d'),
                  'revenue':[1,2,3]})

cur = conn.cursor()
sql = '''
drop table if exists revenue;
CREATE TABLE revenue (
    dt        datetime,
    revenue   int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO revenue(
                    [dt],[revenue]
                    ) 
                    values (?,?)
    ''', 
                    row['dt'], 
                    row['revenue']
               )
conn.commit()
cur.close()
sql = '''select * from revenue t'''
select(sql)

sql = '''select t.dt,t.revenue, 
sum(r.revenue) as cumsum
from revenue t
join revenue r on r.dt <= t.dt 
group by t.dt, t.revenue
'''

select(sql)

# ----------------

conn.close()


