from db import db,connection,metadata,engine
import datetime
import time
time.sleep(5)

source = (db.Table('data_menit', metadata, autoload=True, autoload_with=engine))
sc = source.columns
target = (db.Table('data_jam', metadata, autoload=True, autoload_with=engine))
tg = target.columns

list_value = []

now = datetime.datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:")
start = dt_string+"00:00"
end = dt_string+"59:59"

query = db.select([tg.timestamp]).where(tg.timestamp.between(start,end))
date = (connection.execute(query)).fetchall()

query = db.select([sc.timestamp,sc.fakultas,sc.gedung,sc.meter_id,db.func.avg(sc.power),db.func.avg(sc.tegangan)
                  ,db.func.avg(sc.fasa1),db.func.avg(sc.fasa2),db.func.avg(sc.fasa3),db.func.avg(sc.A)
                  ,db.func.avg(sc.A1),db.func.avg(sc.A2),db.func.avg(sc.A3),db.func.avg(sc.PF),db.func.avg(sc.F)
                  ]).where(sc.timestamp.between(start,end)).group_by(sc.meter_id)

value = (connection.execute(query)).fetchall()

for z in range (0,len(value)) :
  list_value.append(value[z])

count= len(list_value)
if date : 
  for x in range(0,count):
    query = db.update(target).where(target.c.timestamp == start,target.c.meter_id == list_value[x][3]).values(
             {"power":list_value[x][4],"tegangan":list_value[x][5],"fasa1":list_value[x][6],"fasa2":list_value[x][7]
             ,"fasa3":list_value[x][8],"A":list_value[x][9],"A1":list_value[x][10],"A2":list_value[x][11],"A3":list_value[x][12]
             ,"PF":list_value[x][13],"F":list_value[x][14],"quality":1})
    connection.execute(query)
    
  print("update")

else :  
  
  for x in range(0,count):
    query = db.insert(target).values({"timestamp":list_value[x][0],"fakultas":list_value[x][1],"gedung":list_value[x][2],"meter_id":list_value[x][3]
             ,"power":list_value[x][4],"tegangan":list_value[x][5],"fasa1":list_value[x][6],"fasa2":list_value[x][7]
             ,"fasa3":list_value[x][8],"A":list_value[x][9],"A1":list_value[x][10],"A2":list_value[x][11],"A3":list_value[x][12]
             ,"PF":list_value[x][13],"F":list_value[x][14],"quality":1})
    connection.execute(query)

  print("insert")

print('------------------------------------------------')
  