import datetime #импорт библиотеки datetime для приведение столбцов с датой и временем к соответствующим типам
import sqlite3 #импорт библиотеки sqlite3 для подключения к БД
import numpy as np #импорт библиотеки numpy для проверки на пропущенные значения
import pandas as pd #импорт библиотеки padnas для работы с данными
usdrub_quotes = pd.read_csv('C:/Users/79504/Desktop/algo/Si_M15.csv') #загрузка таблицы с котировками
usdrub_quotes.columns = ['candle_date', 'candle_time', 'opening_price','high_price',\
                         'low_price', 'closing_price', 'volume_of_transaction'] # переименование столбцов под naming_convention
usdrub_quotes = usdrub_quotes.astype(int) # приведение значений к int, т.к. доподлинно известно, что цены
                                        # и объёмы торгов этого инструмента на мос. бирже всегда целочисленны
usdrub_quotes = usdrub_quotes.drop(usdrub_quotes[usdrub_quotes['candle_time'] == 000000].index) 
# удаление всех строк, содержащих ячейку 000000 в столбце время по двум причинам: 
# 1. в это время торги не ведутся и эти данные нам не нужны
# 2. такая ячейка воспринимается как 0 и может выдавать некорректные результаты
usdrub_quotes['candle_date'] = pd.to_datetime(usdrub_quotes['candle_date'], format = "%Y%m%d")
usdrub_quotes['candle_date'] = usdrub_quotes['candle_date'].dt.date 
usdrub_quotes['candle_time'] = pd.to_datetime(usdrub_quotes['candle_time'], format = "%H%M%S")
usdrub_quotes['candle_time'] = usdrub_quotes['candle_time'].dt.time
# приведение столбцов с датой и временем к соответствующему типу
print(usdrub_quotes.dtypes) # проверка на соответствие нужным типам данных
print(usdrub_quotes[usdrub_quotes.duplicated()]) # проверка датасета на наличие дубликатов
print(np.where(pd.isnull(usdrub_quotes))) # проверка датасета на наличие пропущенных значений
con = sqlite3.connect('C:/Users/79504/Desktop/Projects/db_quotes', timeout=10) 
# создание и подключение к БД
cur = con.cursor() # создание курсора для выполнения запросов
usdrub_quotes.to_sql(con=con, name='usdrub_quotes', index=False, if_exists = 'replace')
# запись подготовленного датасета в таблицу БД 
data_test = cur.execute('select * from usdrub_quotes') # тестовый запрос
con.commit() # сохранить изменения
cur.fetchall() # возвращение запроса в виде списка  