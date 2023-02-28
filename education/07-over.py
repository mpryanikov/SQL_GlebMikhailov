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


# #### Нарастающий итог:

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

# # 7. Оконные функции

# ## 1. Что такое оконная функция

# ### Нарастающий итог:

sql = '''select t.*,
sum(t.revenue) over (order by t.dt) as cum_sum
from revenue t'''

select(sql)

t = pd.DataFrame({'user_id':[1,1,1,2,2,2],'dt':pd.to_datetime(['2021-04-01','2021-04-02','2021-04-03',
                                                               '2021-04-01','2021-04-02','2021-04-03'],format='%Y-%m-%d'),
                  'revenue':[1,2,3,2,3,4]})

cur = conn.cursor()
sql = '''
drop table if exists revenue;
CREATE TABLE revenue (
    user_id   int,
    dt        datetime,
    revenue   int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO revenue(
                    [user_id],[dt],[revenue]
                    ) 
                    values (?,?,?)
    ''', 
                    row['user_id'],
                    row['dt'], 
                    row['revenue']
               )
conn.commit()
cur.close()
sql = '''select * from revenue t'''
select(sql)

sql = '''select t.*,
sum(t.revenue) over (partition by t.user_id order by t.dt) as cum_sum
from revenue t'''

select(sql)

# ## 2. rank и row_number

t = pd.DataFrame({'user_id':[1,1,1,1,2,2,2],'dt':pd.to_datetime(['2021-04-01','2021-04-02','2021-04-03','2021-04-03',
                                                               '2021-04-03','2021-04-04','2021-04-05'],format='%Y-%m-%d'),
                  'revenue':[1,2,3,1,2,3,4]})

cur = conn.cursor()
sql = '''
drop table if exists revenue;
CREATE TABLE revenue (
    user_id   int,
    dt        datetime,
    revenue   int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO revenue(
                    [user_id],[dt],[revenue]
                    ) 
                    values (?,?,?)
    ''', 
                    row['user_id'],
                    row['dt'], 
                    row['revenue']
               )
conn.commit()
cur.close()
sql = '''select * from revenue t'''
select(sql)

# ### последняя дата активности каждого пользователя:

# ***rank():***

sql = '''select t.*,
rank() over (partition by t.user_id order by t.dt desc) as rnk
from revenue t
'''

select(sql)

sql = '''with 
dt_rank as (
    select t.*,
    rank() over (partition by t.user_id order by t.dt desc) as rnk
    from revenue t
)
select * from dt_rank t
where t.rnk = 1
'''

select(sql)

# ***row_number():***

sql = '''select t.*,
row_number() over (partition by t.user_id order by t.dt desc) as rnk
from revenue t
'''

select(sql)

sql = '''with 
dt_rank as (
    select t.*,
    row_number() over (partition by t.user_id order by t.dt desc) as rnk
    from revenue t
)
select * from dt_rank t
where t.rnk = 1
'''

select(sql)

# #### стандартным способом:

t = pd.DataFrame({'user_id':[1,1,1,2,2,2],'dt':pd.to_datetime(['2021-04-01','2021-04-02','2021-04-03',
                                                               '2021-04-03','2021-04-04','2021-04-05'],format='%Y-%m-%d'),
                  'revenue':[1,2,3,2,3,4]})

cur = conn.cursor()
sql = '''
drop table if exists revenue;
CREATE TABLE revenue (
    user_id   int,
    dt        datetime,
    revenue   int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO revenue(
                    [user_id],[dt],[revenue]
                    ) 
                    values (?,?,?)
    ''', 
                    row['user_id'],
                    row['dt'], 
                    row['revenue']
               )
conn.commit()
cur.close()
sql = '''select * from revenue t'''
select(sql)

sql = '''select t.user_id, 
max(t.dt) as max_dt from revenue t
group by t.user_id'''

select(sql)

sql = '''with 
last_dt as (
    select t.user_id, max(t.dt) as max_dt from revenue t
    group by t.user_id
)
select t.* from revenue t
join last_dt ld on t.user_id = ld.user_id and t.dt = ld.max_dt
order by t.user_id
'''

select(sql)

# ## 3. Топ 3 зарплаты в отделе (задача на интервью)

t = pd.DataFrame({'dep':['a','a','a','a','a',
                         'b','b','b','b','b'],
                  'emp':['aa','bb','cc','dd','ee',
                         'aa','bb','cc','dd','ee'],
                  'sal':[5,5,3,2,1,
                         5,4,3,2,1]})

cur = conn.cursor()
sql = '''
drop table if exists salary;
CREATE TABLE salary (
    dep       varchar(max),
    emp       varchar(max),
    sal       int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO salary(
                    [dep],[emp],[sal]
                    ) 
                    values (?,?,?)
    ''', 
                    row['dep'],
                    row['emp'], 
                    row['sal']
               )
conn.commit()
cur.close()
sql = '''select * from salary t'''
select(sql)

sql = '''select t.*,
rank() over (partition by t.dep order by t.sal desc) as rnk_rank,
dense_rank() over (partition by t.dep order by t.sal desc) as rnk
from salary t
'''

select(sql)

sql = '''with 
salary_rnk as (
    select t.*,
    dense_rank() over (partition by t.dep order by t.sal desc) as rnk
    from salary t
)
select * from salary_rnk t
where t.rnk <= 3
'''

select(sql)

# ## 4. Расчет сессий клиентов (задача из тестового)

# действия клиентов по времени:

user1 = pd.DataFrame({'user_id':[1,1,1,1,1],
                  'dt':pd.to_datetime(['2021-04-01 07:31','2021-04-01 07:35',
                                       '2021-04-01 08:20','2021-04-01 12:31',
                                       '2021-04-03 07:31'],format='%Y-%m-%d %H:%M')})

user2 = pd.DataFrame({'user_id':[2,2,2,2],
                  'dt':pd.to_datetime(['2021-04-01 07:31','2021-04-01 07:35',
                                       '2021-04-01 08:20','2021-04-01 9:10',
                                       ],format='%Y-%m-%d %H:%M')})

user3 = pd.DataFrame({'user_id':[3,3,3],
                  'dt':pd.to_datetime(['2021-04-01 07:31','2021-04-02 07:35',
                                       '2021-04-03 08:20'
                                       ],format='%Y-%m-%d %H:%M')})

t = pd.concat([user1,user2,user3])
# t

cur = conn.cursor()
sql = '''
drop table if exists client_log;
CREATE TABLE client_log (
    user_id   int,
    dt        datetime
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO client_log(
                    [user_id],[dt]
                    ) 
                    values (?,?)
    ''', 
                    row['user_id'],
                    row['dt']
               )
conn.commit()
cur.close()
sql = '''select * from client_log t'''
select(sql)

# ### Надо посчитать количество сессий клиентов:

# Одна сессия, если между действиями проходит меньше часа. Надо посчитать количество сессий клиетов.  
# (для 1 клиента 2-я сессия начинается в 12:31... = 3 сессии  
# 2: 1 сессия, 3: 2 сессии)

# На каждое действие показать предыдущее действие:

# ### ***lag():***

sql = '''select *,
lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt
from client_log t
'''

select(sql)

# #### Сколько времени прошло между текущей активностью и предыдущей:

# + active=""
# # Поскольку мы там видели, что операция расчета разноси двух дат в секундах может давать странные результаты
# # с числами после запятой, то чтобы себя обезопасить и успокоить, можно округлить эту разницу до целых. Вот так:
# #  case when round((julianday(t.dt) - julianday(lag(t.dt) over (partition by t.user_id order by t.dt))) * 24 * 60 * 60) >= 3600
# # then 1 else 0 end as new_session
#
# sql = '''
# select *,
# lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt,
# round((julianday(t.dt) - julianday(lag(t.dt) over (partition by t.user_id order by t.dt))) * 24 * 60 * 60) as dt_diff
# from client_log t
# '''
# -

# <a href="https://learn.microsoft.com/ru-RU/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver15&viewFallbackFrom=sqlallproducts-allversions">
#     DATEDIFF (Transact-SQL) - SQL Server | Microsoft Learn</a>

sql = '''SELECT 
DATEDIFF(second, '2021-04-01 07:31:00.0000000', '2021-04-01 07:35:00.0000000');
'''
select(sql)

sql = '''select *,
lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt,
DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) as dt_diff
from client_log t
'''

select(sql)

# #### Работаем с сессиями (номер сессии, начиная с 0):

sql = '''with 
new_session as (
    select *,
    --lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt,
    --DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) as dt_diff,
    --условия сессий:
    case when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600
        then 1 else 0 end as new_session
    from client_log t
)
--select * from new_session t
--/*
select t.*,
--нарастающий итог (номер сессии, начиная с 0):
sum(t.new_session) over (partition by t.user_id order by t.dt) as session_id
from new_session t
--*/
'''

select(sql)

# #### кол-во активностей в каждой сессии:

sql = '''with 
new_session as (
    select *,
    case when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600
        then 1 else 0 end as new_session
    from client_log t
),
client_sessions as (
    select t.*,
    sum(t.new_session) over (partition by t.user_id order by t.dt) as session_id
    from new_session t 
) 
--select * from client_sessions t
--/*
select t.user_id, t.session_id, count(1) as action_cnt from client_sessions t
group by t.user_id, t.session_id
order by t.user_id, t.session_id
--*/
'''

select(sql)

# #### всего количество сессий:

sql = '''with 
new_session as (
    select *,
    case when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600
        then 1 else 0 end as new_session
    from client_log t
),
client_sessions as (
    select t.*,
    sum(t.new_session) over (partition by t.user_id order by t.dt) as session_id
    from new_session t 
),
client_sessions_agg as (
    select t.user_id, t.session_id, 
    count(1) as action_cnt 
    from client_sessions t
    group by t.user_id, t.session_id
)  
--select * from client_sessions_agg t order by t.user_id, t.session_id
--/*
select count(*) from client_sessions_agg t
--*/
'''

select(sql)

# ## 6. Скользящее среднее

t = pd.DataFrame({'user_id':[1,1,1,1,1,1,
                             2,2,2,2,2],
                  'dt':[1,2,3,4,5,6,
                        1,2,3,4,5],
                  'revenue':[1.0,2,3,4,5,6,
                             3,4,5,6,7]})

cur = conn.cursor()
sql = '''
drop table if exists revenue;
CREATE TABLE revenue (
    user_id   int,
    dt        int,
    revenue   int
);
'''
cur.execute(sql)
conn.commit()
for index,row in t.iterrows():
    cur.execute('''INSERT INTO revenue(
                    [user_id],[dt],[revenue]
                    ) 
                    values (?,?,?)
    ''', 
                row['user_id'],
                row['dt'],
                row['revenue']                
               )
conn.commit()
cur.close()
sql = '''select * from revenue t'''
select(sql)

# #### Среднее для каждой строчки, включая саму строчку и две предыдущие:

sql = '''select t.*,
avg(t.revenue * 1.0) over (
        partition by t.user_id order by t.dt rows between 2 preceding and current row
    ) as moving_avg
from revenue t'''

select(sql)

# ----------------

conn.close()


