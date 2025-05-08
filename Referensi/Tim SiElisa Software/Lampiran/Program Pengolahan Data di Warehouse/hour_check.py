from db import db,connection,metadata,engine
import datetime
import time
time.sleep(15)

target = (db.Table('data_jam', metadata, autoload=True, autoload_with=engine))
tg = target.columns
check = (db.Table('meter_id', metadata, autoload=True, autoload_with=engine))
ck = check.columns

query = db.select([ck.meter_id,ck.fakultas,ck.gedung]).where(ck.meter_id.not_in([103,133]))
check = (connection.execute(query)).fetchall()

def dow() :
  dow = datetime.datetime.today().weekday()
  if dow == 0 :
    dow = 2
  elif dow == 1 :
    dow = 3
  elif dow == 2 :
    dow = 4
  elif dow == 3 :
    dow = 5
  elif dow == 4 :
    dow = 6
  elif dow == 5 :
    dow = 7
  elif dow == 6 :
    dow = 1
  return(dow)

now = datetime.datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:")

start = dt_string+"00:00"
end = dt_string+"59:59"

startx = start

hour_string = now.strftime("%H:")
hour_start = hour_string+"00:00"
hour_end = hour_string+"59:59"

dow = dow()

dat = []

for x in check :
  query = db.select([tg.timestamp]).where(tg.timestamp.between(start,end),tg.meter_id == x[0])
  date = (connection.execute(query)).fetchall()  

  if not date :
    query = db.select([tg.power,tg.tegangan,tg.fasa1,tg.fasa2,tg.fasa3,tg.A,tg.A1,tg.A2,tg.A3,tg.PF,tg.F]).where(tg.meter_id == x[0],db.func.dayofweek(tg.timestamp) == dow,db.func.hour(tg.timestamp).between(hour_start,hour_end)).order_by(tg.timestamp.desc()).limit(7)
    val = (connection.execute(query)).fetchall()  
    
    count = len(val)
    
    power = 0
    tegangan = 0
    fasa1 = 0
    fasa2 = 0
    fasa3 = 0
    A = 0
    A1 = 0
    A2 = 0
    A3 = 0
    PF = 0
    F = 0
    
    for y in val :
      power = power + y[0]
      tegangan = tegangan + y[1]
      fasa1 = fasa1 + y[2]
      fasa2 = fasa2 + y[3]
      fasa3 = fasa3 + y[4]
      A = A + y[5]
      A1 = A1 + y[6]
      A2 = A2 + y[7]
      A3 = A3 + y[8]
      PF = PF + y[9]
      F = F + y[10]
      
    power = power / count
    tegangan = tegangan / count
    fasa1 = fasa1 / count
    fasa2 = fasa2 / count
    fasa3 = fasa3 / count
    A = A / count
    A1 = A1 / count
    A2 = A2 / count
    A3 = A3 / count
    PF = PF / count
    F = F / count
    
    data = (startx,x[1],x[2],x[0],power,tegangan,fasa1,fasa2,fasa3,A,A1,A2,A3,PF,F,0)
    
    dat.append(data)

for value in dat :
   query = db.insert(target).values({"timestamp":value[0],"fakultas":value[1],"gedung":value[2],"meter_id":value[3]
             ,"power":value[4],"tegangan":value[5],"fasa1":value[6],"fasa2":value[7]
             ,"fasa3":value[8],"A":value[9],"A1":value[10],"A2":value[11],"A3":value[12]
             ,"PF":value[13],"F":value[14],"quality":0})

   connection.execute(query)

print('updated')
print('------------------------------------------------')