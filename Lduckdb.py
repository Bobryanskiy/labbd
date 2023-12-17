import duckdb, time

conn = duckdb.connect(database= ':memory:', read_only=False)

conn.execute("CREATE TABLE t AS SELECT * FROM read_csv_auto('nyc_yellow_tiny.csv');")

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    conn.execute("SELECT VendorID, count(*) FROM t GROUP BY 1;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q1: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    conn.execute("SELECT passenger_count, avg(total_amount) FROM t GROUP BY 1;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q2: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    conn.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM t GROUP BY 1, 2;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q3: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    conn.execute('''SELECT passenger_count,
                strftime('%Y', tpep_pickup_datetime),
                round(trip_distance),
                count(*) FROM t
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 desc;''')
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q4: ', m1/10)

conn.close()