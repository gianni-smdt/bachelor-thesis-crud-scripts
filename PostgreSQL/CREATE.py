import csv
import time
import psycopg2
from psycopg2.extras import execute_values

connection = psycopg2.connect(
    dbname = 'test',
    user = 'test',
    password = 'test',
    host = 'localhost',
    port = 5432
)

cursor = connection.cursor()

def create_new_table():
    cursor.execute("""
        drop table if exists test;
        create table test(
            id integer primary key,
            name varchar(100)
        );
    """)
    connection.commit()

def insert(path, batch_size=1000):
    with open(path, newline = '', encoding = 'utf-8') as file:
        reader = csv.DictReader(file)
        data = [(int(row['id']), row['name']) for row in reader]

    start = time.time()

    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        execute_values(
            cursor,
            "insert into test (id, name) values %s",
            batch
        )
    connection.commit()

    end = time.time()

    number_records = len(data)
    duration = end - start
    throughput = number_records / duration

    print(f"Number of inserted records: {number_records}")
    print(f"Total duration: {duration:.6f}s")
    print(f"Throughput: {throughput:.2f}op/s")

if __name__ == '__main__':
    create_new_table()
    insert("<Path to your CSV file>")
    cursor.close()
    conenction.close()
