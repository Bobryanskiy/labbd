import csv, sqlite3, time

con = sqlite3.connect('l1.db')
cur = con.cursor()
cur.execute("DROP TABLE IF EXISTS t")
cur.execute('''CREATE TABLE IF NOT EXISTS t (VendorID Number,
        tpep_pickup_datetime Date,
        tpep_dropoff_datetime Date,
        passenger_count Number,
        trip_distance Number,
        RatecodeID Number,
        store_and_fwd_flag Text,
        PULocationID Number,
        DOLocationID Number,
        payment_type Number,
        fare_amount Number,
        extra Number,
        mta_tax Number,
        tip_amount Number,
        tolls_amount Number,
        improvement_surcharge Number,
        total_amount Number,
        congestion_surcharge Number,
        airport_fee Number);''')

with open('nyc_yellow_tiny.csv', 'r') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['VendorID'], 
              i['tpep_pickup_datetime'],
              i['tpep_dropoff_datetime'],
              i['passenger_count'],
              i['trip_distance'],
              i['RatecodeID'],
              i['store_and_fwd_flag'],
              i['PULocationID'],
              i['DOLocationID'],
              i['payment_type'],
              i['fare_amount'],
              i['extra'],
              i['mta_tax'],
              i['tip_amount'],
              i['tolls_amount'],
              i['improvement_surcharge'],
              i['total_amount'],
              i['congestion_surcharge'],
              i['airport_fee']) for i in dr]
    
cur.executemany('''INSERT INTO t (VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', to_db)
con.commit()

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    cur.execute("SELECT VendorID, count(*) FROM t GROUP BY 1;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q1: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    cur.execute("SELECT passenger_count, avg(total_amount) FROM t GROUP BY 1;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q2: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    cur.execute("SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM t GROUP BY 1, 2;")
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q3: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    cur.execute('''SELECT passenger_count,
                strftime('%Y', tpep_pickup_datetime),
                round(trip_distance),
                count(*) FROM t
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 desc;''')
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q4: ', m1/10)

con.close()