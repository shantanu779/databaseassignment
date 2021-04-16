import json
import csv
import time
from progress.bar import IncrementalBar
with open("Pedestrian.csv","r") as f:
     reader = csv.reader(f)
     next(reader)
     data1 = {"Pedestian":[]}
     data2= {"SensorData":[]}
     bar = IncrementalBar('CSV loading', max = 3623020)
     for row in reader:
         data1["Pedestian"].append({"ID":row[0],"Date_Time":row[1],"Year":row[2],"Month":row[3],"Mdate":row[4],"Day":row[5],"Time":row[6],"Hourly_Counts":row[9],"Sensor_ID":row[7]})
         data2["SensorData"].append({"Sensor_ID":row[7],"Sensor_Name":row[8]})
         bar.next()
bar.finish()
names = data1["Pedestian"]
bar1 = IncrementalBar('Pedestian loading', max = 3623020)
with open("Pedestian.csv","w") as f:
    fielnames=names[0].keys();
    writer = csv.DictWriter(f,fieldnames=fielnames)
    for name in names:
        writer.writerow(name)
        bar1.next()
name1=data2["SensorData"]
bar2 = IncrementalBar('SensorData loading', max = 77)
name2=[]
for name in name1:
    if name not in name2:
        name2.append(name)

with open("SensorData.csv","w") as f:
    fielnames=name1[0].keys();
    writer = csv.DictWriter(f,fieldnames=fielnames)
    #writer.writeheader()
    for name in name2:
        #if name is not in name1
        writer.writerow(name)
        bar2.next()
bar2.finish()
