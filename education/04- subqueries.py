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


# # 5. Подзапросы

# ## 1. Простой подзапрос

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

# ### Дубликаты Id:

sql = '''select 
t.id 
from dupl_test t
group by t.id
having count(1) > 1
'''

select(sql)

sql = '''select * 
from dupl_test t
where t.id in (
    select t.id 
    from dupl_test t
    group by t.id
    having count(1) > 1
)
'''

select(sql)

# #### с созданием промежуточной таблицы:

cur = conn.cursor()
sql = '''
drop table if exists dupls;

select t.id 
into dupls
from dupl_test t
group by t.id
having count(1) > 1
'''
cur.execute(sql)
conn.commit()
cur.close()
sql = '''select * from dupls t'''
select(sql)

sql = '''select * 
from dupl_test t
where t.id in (
    select id from dupls
)
'''

select(sql)

# ### having в подзапросах:

sql = '''select t.id, 
count(1) as cnt
from dupl_test t
group by t.id
having count(1) > 1'''

select(sql)

sql = '''select * from (
    select t.id, 
    count(1) as cnt 
    from dupl_test t
    group by t.id
) t
where t.cnt > 1
'''

select(sql)

# ## 2. CTE (with)

sql = '''select * from (
    select * from (
        select t.id,
        count(1) as cnt 
        from dupl_test t
        group by t.id
    ) t
    where t.cnt > 1
) t
where t.id = 1
'''

select(sql)

sql = '''with 
id_cnt as (
    select t.id,
    count(1) as cnt 
    from dupl_test t
    group by t.id
),
id_cnt_2 as (
    select * 
    from id_cnt t
    where t.cnt > 1
)
select * from id_cnt_2 t
where t.id = 1
'''

select(sql)

# #### закрепим понимание:

cat = '''select t.purpose,

case when t.purpose like '%свадьб%' then 'свадьба'
when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
when t.purpose like '%недвиж%' then 'недвижимость'
else 'другое' end as purpose_cat

from purpose t
'''

print(cat)

sql = f'''select 
t.purpose_cat,
count(1)
from ({cat}) t
group by t.purpose_cat
'''

print(sql)

select(sql)

sql = '''with 
categories as (
    select t.purpose,

    case when t.purpose like '%свадьб%' then 'свадьба'
    when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
    when t.purpose like '%недвиж%' then 'недвижимость'

    else 'другое' end as purpose_cat

    from purpose t
)
select t.purpose_cat,
count(1) 
from categories t
group by t.purpose_cat
'''

select(sql)

# ## 3. Когда лучше создать таблицу, а не использовать подзапрос

cur = conn.cursor()
sql = '''
drop table if exists categories;

select t.purpose,

case when t.purpose like '%свадьб%' then 'свадьба'
when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
when t.purpose like '%недвиж%' then 'недвижимость'

else 'другое' end as purpose_cat

into categories

from purpose t
'''
cur.execute(sql)
conn.commit()
cur.close()
sql = '''select * from categories t'''
select(sql)

sql = '''select 
t.purpose_cat,
count(1)
from categories t
group by t.purpose_cat'''

select(sql)

sql = '''select 
t.purpose, 
count(1) 

from categories t

where t.purpose_cat = 'другое'

group by t.purpose
order by count(1) desc
'''

select(sql)

# #### берёт временную (with categories) а не categories в БД:

sql = '''with 
categories as (
select 1 as p
from purpose t
)
select * from categories t
'''

select(sql)

# ----------------

conn.close()


