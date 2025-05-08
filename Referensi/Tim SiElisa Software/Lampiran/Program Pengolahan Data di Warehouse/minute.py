from db import db,connection,metadata,engine
import time

time.sleep(3)
source = (db.Table('datapengukuran', metadata, autoload=True, autoload_with=engine))
sc = source.columns
target = (db.Table('data_menit', metadata, autoload=True, autoload_with=engine))
tg = target.columns
check = (db.Table('meter_id', metadata, autoload=True, autoload_with=engine))
ck = check.columns

query = db.select([tg.timestamp]).order_by(tg.timestamp.desc()).limit(1)
date = (connection.execute(query)).fetchone()[0]

query = db.select([sc.timestamp,sc.meter_id,(sc.V1*sc.A1*sc.PF1)/1000+(sc.V2*sc.A2*sc.PF2)/1000+(sc.V3*sc.A3*sc.PF3)/1000,sc.VLN
                  ,(sc.V1*sc.A1*sc.PF1)/1000,(sc.V2*sc.A2*sc.PF2)/1000,(sc.V3*sc.A3*sc.PF3)/1000
                  ,sc.A,sc.A1,sc.A2,sc.A3,sc.PF,sc.F]).where(sc.timestamp > date).order_by(sc.id.asc())
value = (connection.execute(query)).fetchall()
count = len(value)

for x in value :
  x = list(x)
  query = db.select([ck.fakultas,ck.gedung]).where(ck.meter_id == x[1])
  lokasi = (connection.execute(query)).fetchone()
  if not lokasi[0] == "" :
    fakultas = lokasi[0]
    gedung = lokasi[1]
    
    query = db.insert(target).values([{"timestamp":x[0],"fakultas":fakultas,"gedung":gedung,"meter_id":x[1],"power":x[2]
            ,"tegangan":x[3],"fasa1":x[4],"fasa2":x[5],"fasa3":x[6],"A":x[7],"A1":x[8],"A2":x[9],"A3":x[10],"PF":x[11],"F":x[12]}])
    connection.execute(query)

if count > 0 :  
    print("{} data inserted".format(count))

else :
    print("no new data")

print('------------------------------------------------')
    