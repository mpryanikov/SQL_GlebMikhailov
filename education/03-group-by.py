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

# # 4. Group By

# ## 1. Сводная таблица

# В сводных таблицах всегда дожен быть *count*

sql = '''select 
t.sex,

count(*) as cnt,

-- поля FLOAT должны, поэтому и не точность
avg(t.credit_amount * 1.0) as credit_amount_avg

from german_credit t
group by t.sex
'''

select(sql)

# + active=""
# df.groupby('sex')['credit_amount'].agg(['count','mean'])
# -

# ### Уникальные значения:

sql = '''select 
count(distinct t.housing), count(t.housing) 
from german_credit t
'''

select(sql)

sql = '''select 
t.housing,

count(*) as cnt,
avg(t.credit_amount * 1.0) as credit_amount_avg

from german_credit t
group by t.housing
'''

select(sql) 

# ## 2. Пропущенные значения (null)

sql = '''select 
count(t.checking_account), count(0) 
from german_credit t
'''

select(sql)

sql = '''select 
t.checking_account,

count(*) as cnt,
avg(t.credit_amount) as credit_amount_avg

from german_credit t
group by t.checking_account
'''

select(sql)

# + active=""
# df.groupby('checking_account',dropna=False)['credit_amount'].count()
# -

sql = '''select 
sum(case when t.checking_account is null then 1 else 0 end) as is_null,
count(case when t.checking_account is null then 1 else null end) as is_null2
from german_credit t
'''

select(sql)

# #### потренируемся:

t = pd.DataFrame({'col1':[1,np.nan,2]})
t = t.replace({np.nan:None})
# t

# +
cur = conn.cursor()
sql = '''
drop table if exists null_test;
CREATE TABLE null_test (
    col1   money
);
'''
cur.execute(sql)
conn.commit()

for index,row in t.iterrows():
    cur.execute('''INSERT INTO null_test(
                    [col1]
                    ) 
                    values (?)
    ''', 
                
                row['col1']
               )
    
conn.commit()
cur.close()

sql = '''select * from null_test t'''
select(sql)
# -

(1 + 2) / 2

(1 + 0 + 2) / 3

sql = '''select avg(t.col1) from null_test t'''

select(sql)

# ### заменим пропуски:

sql = '''select 
t.checking_account,
coalesce(t.checking_account,'no_info')
from german_credit t
'''

select(sql)

# ### coalesce:

t = pd.DataFrame({'col1':[1,np.nan,2],
                  'col2':[np.nan,np.nan,1],
                  'col3':[1,2,3]})
t = t.replace({np.nan:None})
# t

# +
cur = conn.cursor()
sql = '''
drop table if exists null_test;
CREATE TABLE null_test (
    col1        INTEGER,
    col2        INTEGER,
    col3        INTEGER
);
'''
cur.execute(sql)
conn.commit()

for index,row in t.iterrows():
    cur.execute('''INSERT INTO null_test(
                    [col1],[col2],[col3]
                    ) 
                    values (?,?,?)
    ''', 
                row['col1'], 
                row['col2'], 
                row['col3'],
               )
    
conn.commit()
cur.close()

sql = '''select * from null_test t'''
select(sql)
# -

sql = '''select t.*, 
coalesce(t.col1, t.col2, t.col3) as res
from null_test t
'''

select(sql)

# ## 3. Дубликаты

t = pd.DataFrame({'id':[1,1,2],'name':['a','a','b']})
# t

# +
cur = conn.cursor()
sql = '''
drop table if exists dupl_test;
CREATE TABLE dupl_test (
    id        INTEGER,
    name      VARCHAR(max)
);
'''
cur.execute(sql)
conn.commit()

for index,row in t.iterrows():
    cur.execute('''INSERT INTO dupl_test(
                    [id],[name]
                    ) 
                    values (?,?)
    ''', 
                row['id'], 
                row['name']
               )
    
conn.commit()
cur.close()

sql = '''select * from dupl_test t'''
select(sql)
# -

# ### группируем на все поля и посчитаем строки:

sql = '''select 
t.id, t.name, 
count(1) as cnt 
from dupl_test t
group by t.id, t.name
'''

select(sql)

sql = '''select t.id, t.name, 
count(1) as cnt 
from dupl_test t
group by t.id, t.name
having count(1) > 1
'''

select(sql)

# ### дубликат Id:

t = pd.DataFrame({'id':[1,1,2,2,3],
                  'name':['a','b','c','d','e']})
# t

# +
cur = conn.cursor()
sql = '''
drop table if exists dupl_test;
CREATE TABLE dupl_test (
    id        INTEGER,
    name      VARCHAR(max)
);
'''
cur.execute(sql)
conn.commit()

for index,row in t.iterrows():
    cur.execute('''INSERT INTO dupl_test(
                    [id],[name]
                    ) 
                    values (?,?)
    ''', 
                row['id'], 
                row['name']
               )
    
conn.commit()
cur.close()

sql = '''select * from dupl_test t'''
select(sql)
# -

sql = '''select t.id, 
count(1) as cnt from dupl_test t
group by t.id
having count(1) > 1
'''

select(sql)

sql = '''
select * from dupl_test t
where t.id in (1,2)
'''

select(sql)

# #### Используя подзапросы:

sql = '''select 
t.id as cnt 
from dupl_test t
group by t.id
having count(1) > 1
'''

select(sql)

sql = '''select * 
from dupl_test t
where t.id in (
    select t.id as cnt from dupl_test t
    group by t.id
    having count(1) > 1
)
'''

select(sql)

# ## 4. Агрегация

sql = '''select 
year(t.contract_dt) as year,  month(t.contract_dt) as month,

count(1) as credit_cnt,
count(distinct t.client_id) as client_id_unique,
sum(t.credit_amount) as credit_amount_sum,
avg(t.credit_amount * 1.0) as credit_amount_avg

from german_credit t
group by year(t.contract_dt),  month(t.contract_dt)
order by year(t.contract_dt),  month(t.contract_dt)
'''

select(sql)

# ## 5. Создание интервалов (или бинов или бакетов)

# #### Уникальные значения:

sql = '''select 
count(distinct t.credit_amount) 
from german_credit t
'''

select(sql)

# #### Введём диапозоны:

sql = '''select t.credit_amount,

case when t.credit_amount < 1000 then '1. <1000'
when t.credit_amount < 2000 then '2. 1000-2000' 
when t.credit_amount < 3000 then '3. 2000-3000'
when t.credit_amount >= 3000 then '4. >= 3000'
else 'other' end as credit_amount_bin

from german_credit t
'''

select(sql)

sql = '''select 

case 
when t.credit_amount < 1000 then '1. <1000'
when t.credit_amount < 2000 then '2. 1000-2000' 
when t.credit_amount < 3000 then '3. 2000-3000'
when t.credit_amount >= 3000 then '4. >= 3000'
else 'other' end as credit_amount_bin,

count(1) as credit_cnt

from german_credit t

group by case 
when t.credit_amount < 1000 then '1. <1000'
when t.credit_amount < 2000 then '2. 1000-2000' 
when t.credit_amount < 3000 then '3. 2000-3000'
when t.credit_amount >= 3000 then '4. >= 3000'
else 'other' end

order by case 
when t.credit_amount < 1000 then '1. <1000'
when t.credit_amount < 2000 then '2. 1000-2000' 
when t.credit_amount < 3000 then '3. 2000-3000'
when t.credit_amount >= 3000 then '4. >= 3000'
else 'other' end
'''

select(sql)

# ## 6. Переменные в столбцах сводной таблицы

# ### Pivot таблицы:

sql = '''select 
t.housing, 

count(case when t.sex = 'female' then 1 else null end) as female,
count(case when t.sex = 'male' then 1 else null end) as male,

count(1) as cnt 

from german_credit t
group  by t.housing
'''

select(sql)

# #### автоматизируем в Python:

sql = '''select distinct 
t.purpose 
from german_credit t
'''

select(sql)

purpose = list(select(sql)['purpose'].values)
purpose

for p in purpose:
  print(f"count(case when t.purpose = '{p}' then 1 else null end) as {p.lower().replace(' ','').replace('/','')},")

sql = '''select t.housing, 

count(case when t.purpose = 'radio/TV' then 1 else null end) as radiotv,
count(case when t.purpose = 'car' then 1 else null end) as car,
count(case when t.purpose = 'education' then 1 else null end) as education,
count(case when t.purpose = 'furniture/equipment' then 1 else null end) as furnitureequipment,
count(case when t.purpose = 'repairs' then 1 else null end) as repairs,
count(case when t.purpose = 'business' then 1 else null end) as business,
count(case when t.purpose = 'domestic appliances' then 1 else null end) as domesticappliances,
count(case when t.purpose = 'vacation/others' then 1 else null end) as vacationothers,
count(1) as cnt 

from german_credit t
group  by t.housing
'''

select(sql)

# ## 7. Создание категорий из текстовых данных (like)

# #### пример разрозненных данных:

t = pd.DataFrame({'purpose':['машина','машина','машина','на машину','на покупку машины',
                             'автомобиль','на возвращение 2007', 
                             'на свадьбу','свадьба','свадьба','свадьба','для свадьбы',
                             'недвижимость','на покупку недвижимости']})
# t

# +
cur = conn.cursor()
sql = '''
drop table if exists purpose;
CREATE TABLE purpose (
    purpose      VARCHAR(max)
);
'''
cur.execute(sql)
conn.commit()

for index,row in t.iterrows():
    cur.execute('''INSERT INTO purpose(
                    [purpose]
                    ) 
                    values (?)
    ''', 
                    row['purpose']
               )
    
conn.commit()
cur.close()

sql = '''select * from purpose t'''
select(sql)
# -

# #### проверим на уникальные значения:

sql = '''select 
t.purpose, count(1) from purpose t
group by t.purpose
order by count(1) desc
'''

select(sql)

# #### выберем общее:

cat = '''select t.purpose,

case when t.purpose like '%свадьб%' then 'свадьба'
when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
when t.purpose like '%недвиж%' then 'недвижимость'

else 'другое' end as purpose_cat

from purpose t
'''

select(cat)

sql = f'''select 
t.purpose_cat,

count(1)

from (
    select t.purpose,

    case when t.purpose like '%свадьб%' then 'свадьба'
    when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
    when t.purpose like '%недвиж%' then 'недвижимость'

    else 'другое' end as purpose_cat

    from purpose t
) t
group by t.purpose_cat
'''

select(sql)

sql = f'''select 
t.purpose_cat,
count(1)

from ({cat}) t
group by t.purpose_cat
'''

select(sql)

sql = f'''select 
t.purpose, 
count(1) 

from ({cat}) t
where t.purpose_cat = 'другое'

group by t.purpose
order by count(1) desc'''

select(sql)

# ----------------

conn.close()


