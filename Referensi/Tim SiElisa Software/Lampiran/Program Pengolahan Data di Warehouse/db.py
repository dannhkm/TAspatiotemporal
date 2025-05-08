import sqlalchemy as db
import mysql.connector

engine = db.create_engine(mysql+pymysql://energy:energypass@localhost/sielis)
connection = engine.connect()
metadata = db.MetaData()

mydb = mysql.connector.connect(host = 'localhost',user='energy',password='energypass',database="sielis")