from db import mydb
import datetime
import time
time.sleep(10)

mycursor = mydb.cursor()

now = datetime.datetime.now()
dt_string = now.strftime("%Y-%m-%d")

start = dt_string+" 00:00:00"
end = dt_string+" 23:59:59"
start2 = dt_string+" 17:00:00"
end2 = dt_string+" 21:59:59"
start = "'{}'".format(start)
end = "'{}'".format(end)
start2 = "'{}'".format(start2)
end2 = "'{}'".format(end2)

mycursor.execute("select timestamp FROM data_harian where timestamp between "+start+" and "+end)
date = mycursor.fetchall()

query = ("""SELECT 	
		{0} as timestamp,
    fakultas,
    gedung,
		meter_id,
		SUM(power) as energi,
		avg(tegangan) as tegangan,
		avg(fasa1) as fasa1,
		avg(fasa2) as fasa2,
		avg(fasa3) as fasa3,
		avg(pf) as PF,
		avg(f) as F,
		(SELECT SUM(power) as totwbpday
		FROM data_jam jamanwbp 
		WHERE jamanwbp.timestamp=timestamp AND jamanwbp.timestamp >= {1} AND
			jamanwbp.timestamp < {2} AND jamanwbp.meter_id=jaman.meter_id  ) as totwbpday,
		SUM(power) as totkwhday
		FROM data_jam jaman
		WHERE timestamp > {3} AND
			timestamp < {4} 
		GROUP BY meter_id,DATE(timestamp)  ORDER BY `timestamp` ASC""").format(start,start2,end2,start,end)
mycursor.execute(query) 
value = mycursor.fetchall()
list_value = []
for x in range (0,len(value)) :
  list_value.append(value[x])



count= len(list_value)
for x in range(0,count):
  mycursor.execute("select timestamp FROM data_harian where meter_id = {} AND timestamp between ".format(list_value[x][3])+start+" and "+end)
  date = mycursor.fetchall()
  if date : 
    if list_value[x][11] is None :
      wbp = 0
    else :
      wbp = list_value[x][11]
    query =(""" UPDATE data_harian
    SET power = {2}, 
		tegangan = {3},
		fasa1 = {4},
		fasa2 = {5},
		fasa3 = {6},
		PF = {7},
		F = {8},
    totwbpday = {9},
    totkwhday = {10}
    WHERE timestamp = {0} and meter_id = {1}
    """).format(start,list_value[x][3],list_value[x][4],list_value[x][5],list_value[x][6],list_value[x][7],list_value[x][8],list_value[x][9],list_value[x][10],wbp,list_value[x][12])
    mycursor.execute(query)
    mydb.commit()
    print("updated")
    
  else :
    sql = "INSERT INTO data_harian (`timestamp`,`fakultas`,`gedung`,`meter_id`, `power`, `tegangan`, `fasa1`, `fasa2`, `fasa3`, `PF`, `F`, `totwbpday`, `totkwhday`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sql, list_value)
    mydb.commit()
    print("insert")

print('------------------------------------------------')