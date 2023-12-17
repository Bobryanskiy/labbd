import pandas, time
from sqlalchemy import create_engine

engine = create_engine('sqlite:///:memory:')

df = pandas.read_csv('nyc_yellow_tiny.csv')
if 'Airport_fee' in df.columns:
    df = df.drop(columns=['Airport_fee'])

df.to_sql('t', con=engine, index=False)

q1 = "SELECT VendorID, count(*) FROM t GROUP BY 1;"

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    pandas.read_sql(q1, con=engine)
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q1: ', m1/10)

q2 = "SELECT passenger_count, avg(total_amount) FROM t GROUP BY 1;"
m1 = 0
for _ in range(10):
    start = time.perf_counter()
    pandas.read_sql(q2, con=engine)
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q2: ', m1/10)

q3 = "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM t GROUP BY 1, 2;"
m1 = 0
for _ in range(10):
    start = time.perf_counter()
    pandas.read_sql(q3, con=engine)
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q3: ', m1/10)

q4 = '''SELECT passenger_count,
                strftime('%Y', tpep_pickup_datetime),
                round(trip_distance),
                count(*) FROM t
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 desc;'''
m1 = 0
for _ in range(10):
    start = time.perf_counter()
    pandas.read_sql(q4, con=engine)
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q4: ', m1/10)

engine.dispose()