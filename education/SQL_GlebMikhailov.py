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

# # SQL для анализа данных с Глебом Михайловым

# Мой курс на Юдеми https://glebmikhaylov.com/sql  
# Мой канал в Телеграм: https://t.me/mikhaylovgleb  
# Мой канал на Ютюб: https://www.youtube.com/c/GlebMikhaylov  
# Мой сайт: https://glebmikhaylov.com/

# Все файлы и данные можно найти в
# <a href="https://github.com/glebmikha/sql-course">
#    репозитории на GitHub</a>. Основной ноутбук курса лучше открывать сразу на Colab. Вот 
# <a href="https://colab.research.google.com/drive/1Og4wDz-BELxR6izJyWFX-Wn3HVFPHE3W?usp=sharing">
#    ссылка</a>
#  на основной ноутбук со всеми примерами.

# ---


import pandas as pd
import numpy as np

# # 01-connect-create-table

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

# ---

# # 02-select


sql = '''select * from german_credit t'''
select(sql)

# # 3. Select

# ## 1. Псевдонимы

sql = '''
SELECT t.age * 3 AS age_mult3,
   t.housing
FROM german_credit AS t
'''
select(sql)

# ## 2. Базовые операции со столбцами

sql = '''
select t.*, 
    t.age * 3 as age_mult3,
    t.age + t.credit_amount as age_plus_amount,
    t.age * 1.0 / t.credit_amount as age_div_amount,
    t.age as age_2
from german_credit t
'''
select(sql)

# ## 3. Where

sql = '''
select count(1) 
from german_credit t 
where t.contract_dt between 
    Convert(Date, '01.01.2007', 104) and Convert(Date, '31.12.2007', 104)
'''
select(sql)

sql = '''
select * from german_credit t 
where t.contract_dt between 
        Convert(Date, '01.01.2007', 104) and Convert(Date, '31.12.2007', 104)
    and t.purpose in ('car' ,'repairs')
order by t.contract_dt desc, credit_amount 
'''
select(sql)

# ## 5. Case when

# ### Доля клиентов с размером кредита > 1000:

sql = '''
select count(*) from german_credit t
'''
select(sql)

sql = '''
select count(*) from german_credit t
where t.credit_amount > 1000
'''
select(sql)

884/1000

sql = '''
select t.credit_amount,
    case when 
            t.credit_amount > 1000 then 1 
        else 0 
    end as greater_1000_flag,
    iif(t.credit_amount > 1000,1,0) as greater_1000_flag2
from german_credit t
'''
select(sql)

sql = '''
select 
    avg(
        case when t.credit_amount > 1000 then 1.0 else 0 end
        ) as greater_1000_frac
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

# # 03-group-by


sql = '''select * from german_credit t'''
select(sql)

# # 4. Group By

# ## 1. Сводная таблица

# В сводных таблицах всегда дожен быть *count*

sql = '''
select 
    t.sex,
    count(*) as cnt,
    -- поля FLOAT должны, поэтому и не точность
    avg(t.credit_amount * 1.0) as credit_amount_avg
from german_credit t
group by t.sex
'''
select(sql)

df.groupby('sex')['credit_amount'].agg(['count','mean'])

# ### Уникальные значения:

sql = '''
select 
    count(distinct t.housing), 
    count(t.housing) 
from german_credit t
'''
select(sql)

sql = '''
select t.housing,
    count(*) as cnt,
    avg(t.credit_amount * 1.0) as credit_amount_avg
from german_credit t
group by t.housing
'''
select(sql)

# ## 2. Пропущенные значения (null)

sql = '''
select 
    count(t.checking_account), 
    count(0) 
from german_credit t
'''
select(sql)

sql = '''
select t.checking_account,
    count(*) as cnt,
    avg(t.credit_amount) as credit_amount_avg
from german_credit t
group by t.checking_account
'''
select(sql)

df.groupby('checking_account',dropna=False)['credit_amount'].count()

sql = '''
select 
    sum(
        case when t.checking_account is null then 1 else 0 end
        ) as is_null,
    count(
        case when t.checking_account is null then 1 else null end
        ) as is_null2
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

sql = '''
select avg(t.col1) from null_test t
'''
select(sql)

# ### заменим пропуски:

sql = '''
select 
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

sql = '''
select t.*, 
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

sql = '''
select t.id, t.name, 
    count(1) as cnt 
from dupl_test t
group by t.id, t.name
'''
select(sql)

sql = '''
select t.id, t.name, 
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

sql = '''
select t.id, 
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

sql = '''
select t.id as cnt 
from dupl_test t
group by t.id
having count(1) > 1
'''
select(sql)

sql = '''
select * 
from dupl_test t
where t.id in (
        select t.id as cnt 
    from dupl_test t
    group by t.id
    having count(1) > 1
    )
'''
select(sql)

# ## 4. Агрегация

sql = '''
select year(t.contract_dt) as year,  month(t.contract_dt) as month,
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

sql = '''
select 
    count(distinct t.credit_amount) 
from german_credit t
'''
select(sql)

# #### Введём диапозоны:

sql = '''
select t.credit_amount,
    case 
        when t.credit_amount < 1000 then '1. <1000'
        when t.credit_amount < 2000 then '2. 1000-2000' 
        when t.credit_amount < 3000 then '3. 2000-3000'
        when t.credit_amount >= 3000 then '4. >= 3000'
        else 'other' 
    end as credit_amount_bin
from german_credit t
'''
select(sql)

sql = '''
select 
    case 
        when t.credit_amount < 1000 then '1. <1000'
        when t.credit_amount < 2000 then '2. 1000-2000' 
        when t.credit_amount < 3000 then '3. 2000-3000'
        when t.credit_amount >= 3000 then '4. >= 3000'
        else 'other' 
    end as credit_amount_bin,
    count(1) as credit_cnt
from german_credit t
group by 
    case 
        when t.credit_amount < 1000 then '1. <1000'
        when t.credit_amount < 2000 then '2. 1000-2000' 
        when t.credit_amount < 3000 then '3. 2000-3000'
        when t.credit_amount >= 3000 then '4. >= 3000'
        else 'other' 
    end
order by 
    case 
        when t.credit_amount < 1000 then '1. <1000'
        when t.credit_amount < 2000 then '2. 1000-2000' 
        when t.credit_amount < 3000 then '3. 2000-3000'
        when t.credit_amount >= 3000 then '4. >= 3000'
        else 'other' 
    end
'''
select(sql)

# ## 6. Переменные в столбцах сводной таблицы

# ### Pivot таблицы:

sql = '''
select t.housing, 
    count(
        case when t.sex = 'female' then 1 else null end
        ) as female,
    count(
        case when t.sex = 'male' then 1 else null end
        ) as male,
    count(1) as cnt 
from german_credit t
group  by t.housing
'''
select(sql)

# #### автоматизируем в Python:

sql = '''
select distinct t.purpose 
from german_credit t
'''
select(sql)

purpose = list(select(sql)['purpose'].values)
purpose

for p in purpose:
  print(f"count(case when t.purpose = '{p}' then 1 else null end) as {p.lower().replace(' ','').replace('/','')},")

sql = '''
select t.housing, 
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

sql = '''
select t.purpose, 
    count(1) from purpose t
group by t.purpose
order by count(1) desc
'''
select(sql)

# #### выберем общее:

cat = '''
select t.purpose,
    case 
        when t.purpose like '%свадьб%' then 'свадьба'
        when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
        when t.purpose like '%недвиж%' then 'недвижимость'
        else 'другое' 
    end as purpose_cat
from purpose t
'''
select(cat)

sql = '''
select t.purpose_cat,
    count(1)
from (
    select t.purpose,
    case 
        when t.purpose like '%свадьб%' then 'свадьба'
        when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
        when t.purpose like '%недвиж%' then 'недвижимость'
        else 'другое' 
    end as purpose_cat
    from purpose t
    ) t
group by t.purpose_cat
'''
select(sql)

sql = f'''
select t.purpose_cat,
    count(1)
from ({cat}) t
group by t.purpose_cat
'''
select(sql)

sql = f'''
select t.purpose, 
    count(1) 
from ({cat}) t
where t.purpose_cat = 'другое'
group by t.purpose
order by count(1) desc'''
select(sql)

# ----------------

# # 04- subqueries


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

sql = '''
select t.id 
from dupl_test t
group by t.id
having count(1) > 1
'''
select(sql)

sql = '''
select * 
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

sql = '''
select * 
from dupl_test t
where t.id in (
    select id from dupls
)
'''
select(sql)

# ### having в подзапросах:

sql = '''
select t.id, 
    count(1) as cnt
from dupl_test t
group by t.id
having count(1) > 1'''
select(sql)

sql = '''
select * from (
    select t.id, 
        count(1) as cnt 
    from dupl_test t
    group by t.id
) t
where t.cnt > 1
'''
select(sql)

# ## 2. CTE (with)

sql = '''
select * from (
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

sql = '''
with 
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

cat = '''
select t.purpose,
case 
    when t.purpose like '%свадьб%' then 'свадьба'
    when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
    when t.purpose like '%недвиж%' then 'недвижимость'
    else 'другое' 
end as purpose_cat
from purpose t
'''
print(cat)

sql = f'''
select t.purpose_cat,
    count(1)
from ({cat}) t
group by t.purpose_cat
'''

print(sql)

select(sql)

sql = '''
with 
categories as (
    select t.purpose,
    case 
        when t.purpose like '%свадьб%' then 'свадьба'
        when t.purpose like '%машин%' or t.purpose like '%авто%' then 'машина'
        when t.purpose like '%недвиж%' then 'недвижимость'
        else 'другое' 
    end as purpose_cat
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

sql = '''
select t.purpose_cat,
    count(1)
from categories t
group by t.purpose_cat
'''
select(sql)

sql = '''
select t.purpose, 
    count(1) 
from categories t
where t.purpose_cat = 'другое'
group by t.purpose
order by count(1) desc
'''
select(sql)

# #### берёт временную (with categories) а не categories в БД:

sql = '''
with 
categories as (
    select 1 as p
    from purpose t
)
select * from categories t
'''
select(sql)

# ----------------

# # 05-join


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

sql = '''
select 
    t.*, i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
'''
select(sql)

sql = '''
select 
    t.*, i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
where i.item_name is not null
'''
select(sql)

sql = '''
select 
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

sql = '''
select t.*, 
    i.item_name, i.value, i.user_id 
from users t
left join items i on t.id = i.user_id
'''

t = select(sql)
t

t['victory'].sum()

sql = '''
select sum(t.victory) 
from users t
'''
select(sql)

# #### После джойнов:
# 1. Проверяй контрольную сумму
# 2. Проверяй дубликаты

sql = '''
select 
    t.*, i.item_name, i.value, i.user_id 
from users t
join items i on t.id = i.user_id
'''
select(sql)

# #### Как правильно:

sql = '''
select t.id, t.name, t.victory,
    count(i.item_name) as item_cnt,
    coalesce(sum(i.value),0) as value_sum
from users t
left join items i on t.id = i.user_id
group by t.id, t.name, t.victory
'''
select(sql)

# #### Надо перед джойном сгруппировать items:

sql = '''
select t.user_id, 
    count(t.item_name) as item_cnt,
    sum(value) as value_sum from items t
group by t.user_id
'''
select(sql)

sql = '''
with 
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

sql = '''
with 
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

sql = '''
select t.*, i.* 
from users t
left join items i on t.id = i.user_id
'''
select(sql)

sql = '''
select t.*, u.* 
from items t 
left join users u on t.user_id = u.id
'''
select(sql)

sql = '''
select t.*, i.* 
from users t
right join items i on t.id = i.user_id
'''
select(sql)

# ## 6. Full join

sql = '''
select t.*, i.* 
from users t
full join items i on t.id = i.user_id
'''
select(sql)

# Если вдруг не можешь вспомнить как делать full join (да и вообще что либо) -- всегда гугли.  
# <a href='https://stackoverflow.com/questions/1923259/full-outer-join-with-sqlite'>
#     sql - FULL OUTER JOIN with SQLite - Stack Overflow</a>

# #### имитация full join:

sql = '''
select t.*, i.* 
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

sql = '''
select top(5) * 
from german_credit t 
'''
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

sql = '''
select t.*, ctn.data 
from german_credit t 
join clients_task_name ctn on t.client_id = ctn.client_id
'''
select(sql)

# #### генерация заготовок под отчёт:

sql = '''
select 1 as user_id
union all
select 2 as user_id
union all
select 3 as user_id
'''
select(sql)

sql = '''
select convert(date, '01.03.2021', 104) as month
union all
select convert(date, '01.04.2021', 104) as month
'''
select(sql)

sql = '''
with 
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

# # 06-join-practical-examples


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

sql = '''
select year(t.dt) as year,  month(t.dt) as month,
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

sql = '''
select distinct t.client_id 
from german_credit t
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

sql = '''
select sum(t.amount) 
from client_transactions t
'''
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

sql = '''
select t.dt,t.revenue, 
    sum(r.revenue) as cumsum
from revenue t
join revenue r on r.dt <= t.dt 
group by t.dt, t.revenue
'''
select(sql)

# ----------------

# # 07-over


# # 7. Оконные функции

# ## 1. Что такое оконная функция

# ### Нарастающий итог:

sql = '''
select t.*,
    sum(t.revenue) over (order by t.dt) as cum_sum
from revenue t
'''
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

sql = '''
select t.*,
    sum(t.revenue) over (partition by t.user_id order by t.dt) as cum_sum
from revenue t
'''
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

sql = '''
select t.*,
    rank() over (partition by t.user_id order by t.dt desc) as rnk
from revenue t
'''
select(sql)

sql = '''
with 
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

sql = '''
select t.*,
    row_number() over (partition by t.user_id order by t.dt desc) as rnk
from revenue t
'''
select(sql)

sql = '''
with 
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

sql = '''
select t.user_id, 
    max(t.dt) as max_dt 
from revenue t
group by t.user_id
'''
select(sql)

sql = '''
with 
last_dt as (
    select t.user_id, 
        max(t.dt) as max_dt 
    from revenue t
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

sql = '''
select t.*,
    rank() over (partition by t.dep order by t.sal desc) as rnk_rank,
    dense_rank() over (partition by t.dep order by t.sal desc) as rnk
from salary t
'''
select(sql)

sql = '''
with 
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

sql = '''
select *,
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

sql = '''
SELECT DATEDIFF(second, '2021-04-01 07:31:00.0000000', '2021-04-01 07:35:00.0000000');
'''
select(sql)

sql = '''
select *,
    lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt,
    DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) as dt_diff
from client_log t
'''
select(sql)

# #### Работаем с сессиями (номер сессии, начиная с 0):

sql = '''
with 
new_session as (
    select *,
        --lag(t.dt) over (partition by t.user_id order by t.dt) as prev_dt,
        --DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) as dt_diff,
        --условия сессий:
        case 
            when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600 then 1 
            else 0 
        end as new_session
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

sql = '''
with 
new_session as (
    select *,
        case 
            when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600 then 1 
            else 0 
        end as new_session
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

sql = '''
with 
new_session as (
    select *,
        case 
            when DATEDIFF(second, lag(t.dt) over (partition by t.user_id order by t.dt), t.dt) >= 3600 then 1 
            else 0 
        end as new_session
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

sql = '''
select t.*,
avg(t.revenue * 1.0) over (
        partition by t.user_id order by t.dt rows between 2 preceding and current row
    ) as moving_avg
from revenue t
'''
select(sql)

# ----------------

# # 08-conclusion


sql = '''drop table if exists Employee
create table Employee(Id int, Salary int)
insert into Employee(Id, Salary) values (1, 100)
insert into Employee(Id, Salary) values (2, 200)
insert into Employee(Id, Salary) values (3, 300)
'''

# поставим ; после каждой строки:

sql = ';\n'.join(sql.split('\n'))
print(sql)

# ---

conn.close()

# # 8. Заключение

# ## 2. Где тренироваться

# https://sql-ex.ru/

# https://leetcode.com/ - платный (дорогой)

# <a href="https://stepik.org/course/63054/promo">
#    Интерактивный тренажер по SQL · Stepik</a>


