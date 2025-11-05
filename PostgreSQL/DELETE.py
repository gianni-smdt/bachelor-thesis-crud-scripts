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

def delete(batch_size = 1000):
  start = time.time()
  cursor.execute("select id from test")
  data = [row[0] for row in cursor.fetchall()]
  number_deletes = 0

  for i in range(0, len(data), batch_size):
    batch = data[i:i + batch_size]
    cursor.execute(
      'delete from test where id = any(%s)',
      (batch, )
    )
    number_deletes += len(batch)
  
  connection.commit()
  
  end = time.time()
  duration = end - start
  throughput = number_deletes/duration
  
  print(f"Number of inserted records: {number_records}")
  print(f"Total duration: {duration:.6f}s")
  print(f"Throughput: {throughput:.2f}op/s"

if __name__ == '__main__':
  delete()
  cursor.close()
  connection.close()
