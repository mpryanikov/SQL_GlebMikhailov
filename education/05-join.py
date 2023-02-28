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


# # 6. Джойны

users = pd.DataFrame({'id':[1,2,3],'name':['gleb','jon snow','tyrion']})

items = pd.DataFrame({'user_id':[1,3,3],'item_name':['hleb','gold','wine'],'value':[5,100,20]})

cur = conn.cursor()
sql = '''
drop table if exists users;
CREATE TABLE users (
    id        INTEGER,
    name      VARCHAR(max)
);
'''
cur.execute(sql)
conn.commit()
for index,row in users.iterrows():
    cur.execute('''INSERT INTO users(
                    [id],[name]
                    ) 
                    values (?,?)
    ''', 
                    row['id'], 
                    row['name']
               )
conn.commit()
cur.close()
sql = '''select t.* from users t'''
select(sql)

cur = conn.cursor()
sql = '''
drop table if exists items;
CREATE TABLE items (
    user_id        INTEGER,
    item_name      VARCHAR(max), 
    value          MONEY
);
'''
cur.execute(sql)
conn.commit()
for index,row in items.iterrows():
    cur.execute('''INSERT INTO items(
                    [user_id],[item_name],[value]
                    ) 
                    values (?,?,?)
    ''', 
                    row['user_id'], 
                    row['item_name'],
                    row['value']
               )
conn.commit()
cur.close()
sql = '''select t.* from items t'''
select(sql)

# ## 2. Лефт и иннер джойн

sql = '''select 
t.*, i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
'''

select(sql)

sql = '''select 
t.*, i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
where i.item_name is not null
'''

select(sql)

sql = '''select 
t.*, i.item_name 
from users t
join items i on t.id = i.user_id
'''

select(sql)

# ## 3. Агрегируй перед джойном!

users = pd.DataFrame({'id':[1,2,3],'name':['gleb','jon snow','tyrion'],
                      'victory':[2,10,1]})

# +
cur = conn.cursor()
sql = '''
drop table if exists users;
CREATE TABLE users (
    id        INTEGER,
    name      VARCHAR(max),
    victory   INTEGER
);
'''
cur.execute(sql)
conn.commit()

for index,row in users.iterrows():
    cur.execute('''INSERT INTO users(
                    [id],[name],[victory]
                    ) 
                    values (?,?,?)
    ''', 
                    row['id'], 
                    row['name'],
                    row['victory']
               )
    
conn.commit()
cur.close()
sql = '''select t.* from users t'''
select(sql)
# -

sql = '''select t.*, 
i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
'''

t = select(sql)
t

t['victory'].sum()

sql = '''select sum(t.victory) from users t'''

select(sql)

# #### После джойнов:
# 1. Проверяй контрольную сумму
# 2. Проверяй дубликаты

sql = '''select t.*, i.item_name, i.value, i.user_id 
from users t
join items i on t.id = i.user_id
'''

select(sql)

# #### Как правильно:

sql = '''select 
t.id, t.name, t.victory,

count(i.item_name) as item_cnt,
coalesce(sum(i.value),0) as value_sum

from users t
left join items i on t.id = i.user_id
group by t.id, t.name, t.victory
'''

select(sql)

# #### Надо перед джойном сгруппировать items:

sql = '''select t.user_id, 
count(t.item_name) as item_cnt,
sum(value) as value_sum from items t
group by t.user_id'''

select(sql)

sql = '''with 
items_agg as (
    select t.user_id, 
    count(t.item_name) as item_cnt,
    sum(value) as value_sum 
    from items t
    group by t.user_id
)
select t.id, t.name, t.victory,

coalesce(i.item_cnt,0) as item_cnt,
coalesce(i.value_sum,0) as value_sum

from users t

left join items_agg i on t.id = i.user_id
'''

select(sql)

# ## 4. Как не надо делать джойны

# #### всегда надо писать псевдонимы:

sql = '''with 
items_agg as (
    select t.user_id, 
    count(t.item_name) as item_cnt,
    sum(value) as value_sum 
    from items t
    group by t.user_id
)
select t.id, t.name, t.victory,

coalesce(item_cnt,0) as item_cnt,
coalesce(value_sum,0) as value_sum

from users t

left join items_agg i on t.id = i.user_id
'''

select(sql)

# ## 5. Никогда не используй right join!

users = pd.DataFrame({'id':[1,2,3],'name':['gleb','jon snow','tyrion']})

items = pd.DataFrame({'user_id':[1,3,3,4],'item_name':['hleb','gold','wine','sword'],'value':[5,100,20,50]})

cur = conn.cursor()
sql = '''
drop table if exists users;
CREATE TABLE users (
    id        INTEGER,
    name      VARCHAR(max)
);
'''
cur.execute(sql)
conn.commit()
for index,row in users.iterrows():
    cur.execute('''INSERT INTO users(
                    [id],[name]
                    ) 
                    values (?,?)
    ''', 
                    row['id'], 
                    row['name']
               )
conn.commit()
cur.close()
sql = '''select t.* from users t'''
select(sql)

cur = conn.cursor()
sql = '''
drop table if exists items;
CREATE TABLE items (
    user_id        INTEGER,
    item_name      VARCHAR(max), 
    value          MONEY
);
'''
cur.execute(sql)
conn.commit()
for index,row in items.iterrows():
    cur.execute('''INSERT INTO items(
                    [user_id],[item_name],[value]
                    ) 
                    values (?,?,?)
    ''', 
                    row['user_id'], 
                    row['item_name'],
                    row['value']
               )
conn.commit()
cur.close()
sql = '''select t.* from items t'''
select(sql)

sql = '''select t.*, i.* 
from users t
left join items i on t.id = i.user_id
'''

select(sql)

sql = '''select t.*, u.* 
from items t 
left join users u on t.user_id = u.id
'''

select(sql)

sql = '''select t.*, i.* 
from users t
right join items i on t.id = i.user_id'''

select(sql)

# ## 6. Full join

sql = '''select t.*, i.* 
from users t
full join items i on t.id = i.user_id'''

select(sql)

# Если вдруг не можешь вспомнить как делать full join (да и вообще что либо) -- всегда гугли.  
# https://stackoverflow.com/questions/1923259/full-outer-join-with-sqlite

# #### имитация full join:

sql = '''select t.*, i.* 
from users t
left join items i on t.id = i.user_id
union 
select u.*, t.* 
from items t 
left join users u on t.user_id = u.id
'''

select(sql)

# ## 7. Фишки с inner join

# #### сопоставление с "присланным" файлом:

sql = '''select top(5) * from german_credit t '''

select(sql)

clients = pd.DataFrame({'client_id':[200,45],'data':[1.0, 2.0]})

cur = conn.cursor()
sql = '''
drop table if exists clients_task_name;
CREATE TABLE clients_task_name (
    client_id        int,
    data             int
);
'''
cur.execute(sql)
conn.commit()
for index,row in clients.iterrows():
    cur.execute('''INSERT INTO clients_task_name(
                    [client_id],[data]
                    ) 
                    values (?,?)
    ''', 
                    row['client_id'], 
                    row['data']
               )
conn.commit()
cur.close()
sql = '''select t.* from clients_task_name t'''
select(sql)

sql = '''select t.*, ctn.data 
from german_credit t 
join clients_task_name ctn on t.client_id = ctn.client_id
'''

select(sql)

# #### генерация заготовок под отчёт:

sql = '''select 1 as user_id
union all
select 2 as user_id
union all
select 3 as user_id'''

select(sql)

sql = '''
select convert(date, '01.03.2021', 104) as month
union all
select convert(date, '01.04.2021', 104) as month
'''

select(sql)

sql = '''with 
users as (
    select 1 as user_id
    union all
    select 2 as user_id
    union all
    select 3 as user_id
),
month as (
    select convert(date, '01.03.2021', 104) as month
    union all
    select convert(date, '01.04.2021', 104) as month
)
select * from users t
join month m on 1=1
'''

select(sql)

# ----------------

conn.close()


