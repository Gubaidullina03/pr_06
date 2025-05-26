# pr_06
# Аналитика с использованием сложных типов данных. Поиск и анализ продаж

## Цель:
Рассмотреть данные переписи населения США об использовании общественного транспорта относительно ZIP-кода.
Требуется проанализировать, имеется ли корреляция между уровнем использования общественного транспорта с продажами в
организации в определенном регионе.

## Задачи:
1. Загрузить набор данных public_transportation_statistics_by_zip_code.csv с GitHub:
https://github.com/BosenkoTM/SQL-for-Begginer-DataAnalytics/blob/main/Datasets/public_transportation_statistics_by_zip_code.csv
2. Скопировать данные public_transportation_statistics_by_zip_code.csv в базу данных клиентов организации, создав для них таблицу в наборе данных.
3. Найти максимальное и минимальное процентное соотношение в данных. Значения ниже 0 принять как пустое значение.
4. Рассчитать средний объем продаж для клиентов, проживающих в регионах с высоким уровнем использования общественного транспорта (более 10%), а также с низким уровнем использования общественного транспорта
(менее или равным 10%).
5. Загрузить данные в pandas и построить гистограмму распределения
(использовать my_data.plot.hist(y='public_transportation_pct') для построения
гистограммы).
6. Сгруппировать клиентов по их zip_code, и посмотрите на среднее количество транзакций на одного клиента. Экспортировать эти данные в Excel и создайте диаграмму рассеяния, чтобы лучше понять взаимосвязь между использованием общественного транспорта и продажами.
7. На основании этого анализа дать рекомендации руководству компании при рассмотрении возможностей расширения

## Выполнение задания
Подключимся к библиотеке psycopg2 и установим соединение с PostgreSQL:
```python
%pip install psycopg2
```
```python
import psycopg2
from psycopg2 import Error
```
## 1. Подключимся к заранее созданной базе  данных pr6 и создим таблицу public_transportation_statistics_by_zip_code, с которой будем в дальнейшем работать.
```python
import psycopg2

def get_connection(database_name):
    # Прописываем функцию для подключения к нашей базе данных
    connection = psycopg2.connect(user="postgres",
                                  password="1",
                                  host="localhost",
                                  port="5432",
                                  database=database_name)
    return connection

def close_connection(connection):
    # Также сразу пишем функцию для закрытия подключения
    if connection:
        connection.close()
        print("Соединение с PostgreSQL закрыто")

try:
    connection = get_connection("pr6")
    cursor = connection.cursor()

    # Создаем таблицу 
    create_table_query = '''
    CREATE TABLE public_transportation_statistics_by_zip_code(
        zip_code character varying(10) NOT NULL PRIMARY KEY,
        public_transportation_pct numeric(15,2) NOT NULL,
        public_transportation_population integer
    );
    '''
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица 'public_transportation_statistics_by_zip_code' успешно создана")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрываем подключения к базе данных
    if connection:
        close_connection(connection)
```
Получим результат:

![пр 6 таблица успешно создана](https://github.com/user-attachments/assets/7022e85a-19c4-4bcc-9736-edfcfd29f23d)


## 2. Скопируем данные public_transportation_statistics_by_zip_code.csv в базу данных клиентов организации, в предворительно созданную нами таблицу.
```python
import os  
import psycopg2  
from psycopg2 import Error

try:
    connection = get_connection("pr6")
    cursor = connection.cursor()

# Прописываем путь к месту, где хранится CSV-файл
    csv_file_path = r"C:\Users\626\Downloads\public_transportation_statistics_by_zip_code.csv"

    # Проверяем сущестует ли файл
    if not os.path.exists(csv_file_path):
        print(f"ОШИБКА: Файл '{csv_file_path}' не найден.")


        # Вставляем данные из CSV-файла с использованием COPY
    with open(csv_file_path, 'r') as file:
        copy_query = """
        COPY public_transportation_statistics_by_zip_code(zip_code, public_transportation_pct, public_transportation_population)
        FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """
        cursor.copy_expert(copy_query, file)
        connection.commit()
        print("Данные успешно вставлены в таблицу 'public_transportation_statistics_by_zip_code'")

except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрываем подключения к базе данных
    if connection:
        close_connection(connection)
```
Получим результат:

![пр 6 вставка данных в табл](https://github.com/user-attachments/assets/2c99fc2a-2913-4ed2-936e-daeb01ef1512)


## 3. Найдем максимальное и минимальное процентное соотношение в данных. Значения ниже 0 будем принимать как пустое значение.
```python
import psycopg2 
from psycopg2 import Error
try:
    connection = get_connection("pr6")
    cursor = connection.cursor()

    # Составим запрос для того чтобы найти максимальное и минимальное значение процентного соотношения
    query = """
    SELECT 
    MAX(public_transportation_pct) AS max_pct,
    MIN(public_transportation_pct) AS min_pct
    FROM public_transportation_statistics_by_zip_code
    WHERE public_transportation_pct >= 0;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    max_pct, min_pct = result
    print(f"Максимальное процентное соотношение: {max_pct}")
    print(f"Минимальное процентное соотношение: {min_pct}")
    
except (Exception, psycopg2.Error) as error:
    print("Ошибка при подключении или работе с PostgreSQL:", error)

finally:
    # Закрываем подключения к базе данных
    if connection:
        close_connection(connection)
```

Получим результат:

![пр 6 пункт 3](https://github.com/user-attachments/assets/287cf19e-981c-4e60-b431-1223a02e222f)




## Вывод
Рассмотрела данные переписи населения США об использовании общественного транспорта относительно ZIP-кода.
Проанализировала, имеется ли корреляция между уровнем использования общественного транспорта с продажами в
организации в определенном регионе.


