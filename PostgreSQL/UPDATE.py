import time
import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker

connection = psycopg2.connect(
  dbname = 'test',
  user = 'test',
  password = 'test',
  host = 'localhost',
  port = 5432
)

fake = Faker()
cursor = connection.cursor()

def update(batch_size = 1000):
  cursor.execute("select id from test")
  data = [row[0] for row in cursor.fetchall()]
  start = time.time()
  number_records = 0

  for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_sizee]
    new_values = [(fake.name(), id) for id in batch]
   
    execute_batch(
      cursor,
      'update test set name = %s where id = %s',
      new_values,
      page_size = batch_size
    )
    number_records += len(new_values)

  connection.commit()

  end = time.time()
  duration = end - start
  throughput = number_records/duration

  print(f"Number of inserted records: {number_records}")
  print(f"Total duration: {duration:.6f}s")
  print(f"Throughput: {throughput:.2f}op/s"

if __name__ == '__main__':
  update()
  cursor.close()
  connection.close()
