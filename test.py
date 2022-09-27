import json
import psycopg2
from psycopg2.extras import execute_values
import re
conn_args=dict(
            host='localhost',
            user='kirill',
            password='kirill',
            dbname='mydb',
            port=5432)
conn = psycopg2.connect(**conn_args)
#s3.load_string(string_data=s, key=key, bucket_name=b)
cur = conn.cursor()
#obj = s3.get_key(file_processing, b)
cur.execute("DELETE FROM {}".format('prepared_events'))
with open('try.json', 'r') as f:
    lst = [i for i in f.read().splitlines()]
    data = [json.loads(i) for i in lst]
copy_sql = """
    insert into prepared_events({cols}) values %s;
"""
query = copy_sql
columns = data[0].keys()
query = query.format(cols=','.join(columns))
values = ((d[column] for column in columns) for d in data)
cur.executemany(query, [(tuple(row),) for row in values])
try:
    conn.commit()
except:
    raise
print('success')
cur.close()
conn.close()
