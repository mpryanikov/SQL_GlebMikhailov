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

# ### надо сгенерить заготовку, чтобы были все месяцы:

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

# #### Прочие примеры:

# <a href="https://info-comp.ru/generating-dates-t-sql">
#     Как получить последовательность дат в указанном промежутке на T-SQL</a>

cur = conn.cursor()
sql = '''
   --Табличная функция для генерации последовательности дат (способ 2 – WITH)
   CREATE FUNCTION GeneratingDates (
	@DateStart DATE, -- Дата начала
	@DateEnd DATE	 -- Дата окончания
   )
   RETURNS @ListDates TABLE (dt DATE) 
   AS
   BEGIN

	--Рекурсивное обобщенное табличное выражение.
	WITH Dates AS
	(
		SELECT @DateStart AS DateStart -- Задаем якорь рекурсии
	
		UNION ALL

		SELECT DATEADD(DAY, 1, DateStart) AS DateStart -- Увеличиваем значение даты на 1 день
		FROM Dates
		WHERE DateStart < @DateEnd -- Прекращаем выполнение, когда дойдем до даты окончания
	)
	INSERT INTO @ListDates
		SELECT DateStart 
		FROM Dates
		OPTION (MAXRECURSION 0); 
		/*
			Значением 0 снимаем серверное ограничение на количество уровней рекурсии (которое по умолчанию равно 100), 
			чтобы иметь возможность формировать даты в большом диапазоне.
		*/
     RETURN
   END
'''
cur.execute(sql)
conn.commit()
cur.close()

sql = '''
declare @min date
declare @max date
select @min = min(t.dt) from client_transactions t
select @max = max(t.dt) from client_transactions t
SELECT * FROM GeneratingDates(@min,@max);
'''

select(sql)

# <a href="https://learntutorials.net/ru/sql-server/topic/3232/создание-диапазона-дат">
#     Microsoft SQL Server Создание диапазона дат</a>

sql = '''
Declare   @FromDate   Date,
          @ToDate     Date
select @FromDate = min(t.dt) from client_transactions t
select @ToDate = max(t.dt) from client_transactions t;          

With 
   E1(N) As (Select 1 From (Values (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)) DT(N)),
   E2(N) As (Select 1 From E1 A Cross Join E1 B),
   E4(N) As (Select 1 From E2 A Cross Join E2 B),
   E6(N) As (Select 1 From E4 A Cross Join E2 B),
   Tally(N) As
   (
        Select    Row_Number() Over (Order By (Select Null)) 
        From    E6
   )
Select   year(DateAdd(Day, N - 1, @FromDate)) year, month(DateAdd(Day, N - 1, @FromDate)) month
From     Tally
Where    N <= DateDiff(Day, @FromDate, @ToDate) + 1
'''

select(sql)

# ----------------

conn.close()


