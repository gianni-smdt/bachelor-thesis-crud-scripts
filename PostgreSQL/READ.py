import time
import psycopg2

connection = psycopg2.connect(
  dbname = 'test',
  user = 'test',
  password = 'test',
  host = 'localhost',
  port = 5432
)

cursor = connection.cursor()

def select(batch_size = 1000):
  start = time.time()
  cursor.execute("select * from test")
  
  number_records = 0
 
  while True:
    batch = cursor.fetchmany(batch_size)
    if not batch:
      break
  
  number_records += len(batch)
  end = time.time()
  duration = end - start
  throughput = number_records/duration

  print(f"Number of inserted records: {number_records}")
  print(f"Total duration: {duration:.6f}s")
  print(f"Throughput: {throughput:.2f}op/s"

if __name__ == '__main__':
  select()
  cursor.close()
  connection.close()
