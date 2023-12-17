from sqlalchemy import create_engine, Column, Integer, Float, String, Date, select, func, desc
from sqlalchemy.orm import sessionmaker, declarative_base
import time
import pandas

engine = create_engine('sqlite:///:memory:', echo=False)
Base = declarative_base()

class Table(Base):
    __tablename__ = 't'
    __table_args__ = {'sqlite_autoincrement': True}

    id = Column(Integer, primary_key=True, nullable=False)
    VendorID = Column(Integer)
    tpep_pickup_datetime = Column(Date)
    tpep_dropoff_datetime = Column(Date)
    passenger_count = Column(Integer)
    trip_distance = Column(Float)
    RatecodeID = Column(Integer)
    store_and_fwd_flag = Column(String)
    PULocationID = Column(Integer)
    DOLocationID = Column(Integer)
    payment_type = Column(Integer)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    airport_fee = Column(Float)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

df = pandas.read_csv('nyc_yellow_tiny.csv')
if 'Airport_fee' in df.columns:
    df = df.drop(columns=['Airport_fee'])
df.to_sql(con=engine, index_label='id', name=Table.__tablename__, if_exists='replace')
session.commit()

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    session.execute(select(Table.VendorID, func.count("*")).group_by(Table.VendorID))
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q1: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    session.execute(select(Table.passenger_count, func.avg(Table.total_amount)).group_by(Table.passenger_count))
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q2: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    session.execute(select(Table.passenger_count, func.strftime("%Y", Table.tpep_pickup_datetime), func.count("*")).group_by(Table.passenger_count, func.strftime("%Y", Table.tpep_pickup_datetime)))
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q3: ', m1/10)

m1 = 0
for _ in range(10):
    start = time.perf_counter()
    session.execute(select(Table.passenger_count, func.strftime('%Y', Table.tpep_pickup_datetime),
                            func.round(Table.trip_distance), func.count("*"))
                            .group_by(Table.passenger_count,
                                    func.strftime('%Y', Table.tpep_pickup_datetime),
                                    func.round(Table.trip_distance))
                            .order_by(func.strftime('%Y', Table.tpep_pickup_datetime), desc(func.count("*"))))
    finish = time.perf_counter()
    m1 += finish - start
print('Elapsed time q4: ', m1/10)

session.close()