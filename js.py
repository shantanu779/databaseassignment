import json
import csv
import time
from progress.bar import IncrementalBar

with open("Pedestrian.csv","r") as f:
     reader = csv.reader(f)
     next(reader)
     bar = IncrementalBar('CSV loading', max = 3574594)
     data1 = {"Pedestian":[]}
     data2= {"SensorData":[]}
     for row in reader:
         Hourly_ = row[9].replace(',','')
         data1["Pedestian"].append({"ID":int(row[0]),"Date_Time":row[1],"Year":int(row[2]),"Month":row[3],"Mdate":int(row[4]),"Day":row[5],"Time":int(row[6]),"Hourly_Counts":int(Hourly_),"Sensor_ID":int(row[7])})
         data2["SensorData"].append({"Sensor_ID":int(row[7]),"Sensor_Name":row[8]})
         bar.next()
bar.finish()
name1=data2["SensorData"]
bar2 = IncrementalBar('SensorData filtering', max = 77)
name2=[]
for name in name1:
    if name not in name2:
        name2.append(name)
        bar.next()
with open("Pedestian.json","w") as f:
    json.dump(data1, f, indent=4)
with open("SensorData.json","w") as f:
    json.dump(name2, f, indent=4)
