import psycopg2, pandas, time
from sqlalchemy import create_engine

db_params = {
    'dbname' : 'postgres',
    'user' : 'postgres',
    'password' : 'postgres',
    'host' : 'localhost',
    'port' : '5432'
}

try:
    connection = psycopg2.connect(**db_params)
    cursor  = connection.cursor()

    engine = create_engine(f"postgresql+psycopg2://postgres:{'postgres'}@localhost:5432/postgres")
    df = pandas.read_csv('nyc_yellow_tiny.csv')
    if 'Airport_fee' in df.columns:
        df = df.drop(columns=['Airport_fee'])
    df.to_sql('t', con=engine, index=False, if_exists='replace')
    cursor.execute('ALTER TABLE t ALTER COLUMN tpep_pickup_datetime TYPE DATE USING tpep_pickup_datetime::DATE;')
    cursor.execute('ALTER TABLE t ALTER COLUMN tpep_dropoff_datetime TYPE DATE USING tpep_dropoff_datetime::DATE;')
        
    connection.commit()

    m1 = 0
    for _ in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT 't.VendorID', count(*) FROM t GROUP BY 1;")
        finish = time.perf_counter()
        m1 += finish - start
    print('Elapsed time q1: ', m1/10)

    m1 = 0
    for _ in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT t.passenger_count, avg(t.total_amount) FROM t GROUP BY 1;")
        finish = time.perf_counter()
        m1 += finish - start
    print('Elapsed time q2: ', m1/10)

    m1 = 0
    for _ in range(10):
        start = time.perf_counter()
        cursor.execute("SELECT t.passenger_count, date_part('year', t.tpep_pickup_datetime), count(*) FROM t GROUP BY 1, 2;")
        finish = time.perf_counter()
        m1 += finish - start
    print('Elapsed time q3: ', m1/10)

    m1 = 0
    for _ in range(10):
        start = time.perf_counter()
        cursor.execute('''SELECT t.passenger_count,
                    extract(year from t.tpep_pickup_datetime),
                    round(t.trip_distance),
                    count(*) FROM t
                    GROUP BY 1, 2, 3
                    ORDER BY 2, 4 desc;''')
        finish = time.perf_counter()
        m1 += finish - start
    print('Elapsed time q4: ', m1/10)
    cursor.close()
    connection.close()

except psycopg2.Error as e:
    print('error con to bd:', e)