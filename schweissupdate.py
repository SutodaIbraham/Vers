# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 08:31:11 2022

@author: ibraham
"""

import glob # 
import pandas as pd 
import pyodbc
from datetime import datetime 
server = 'ltp077' 
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')

filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\CASTECH_Schweissanlage\Messdaten/*.csv")

for filename in filenames:
    try:
     p = pd.read_csv(filename)
     if len(p.columns)==1:
         p = pd.read_csv(filename, sep = ',"').apply(lambda x: x.str.strip('"') )
     p.columns = ["Date1","Time1","Program","PreConnectionPulse","PreConnectionPower","PreConnectionPressure","RampTime","PulseInterval","Pulse",
"ConnectionTime","Power1","Pressure","Mode1","StartDelay","ConnectionInterval","Trimmingcut","EndRotation","ZAxisLength","BlowAirTime","VoltageRead",
"CurrentCalculate","CurrentRead","CurrentDelta","PreConnectionCurrentRead","MovementSet","MovementRead","MovementDelta","ForceSet","ForceRead","ForceDelta",
"PreConnectionForceRead","ConnectionNumber","CycleNumber","CyclePosition","StatorCode_BarCode","Quality"]
     for row in p.itertuples(): 
       conn.execute('''INSERT INTO dbo.Schweissanlage(Date1, Time1, Program, PreConnectionPulse, PreConnectionPower,
                  PreConnectionPressure, RampTime, PulseInterval, Pulse, ConnectionTime, Power1, Pressure, Mode1,
                  StartDelay, ConnectionInterval, Trimmingcut, EndRotation, ZAxisLength, BlowAirTime, VoltageRead,
                  CurrentCalculate, CurrentRead, CurrentDelta, PreConnectionCurrentRead, MovementSet, MovementRead,
                  MovementDelta,ForceSet,ForceRead,ForceDelta, PreConnectionForceRead, ConnectionNumber,CycleNumber,
                  CyclePosition, StatorCode_BarCode,Quality)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Date1, row.Time1, row.Program, row.PreConnectionPulse, row.PreConnectionPower, row.PreConnectionPressure,
                  row.RampTime, row.PulseInterval, row.Pulse, row.ConnectionTime, row.Power1, row.Pressure, row.Mode1, row.StartDelay,
                  row.ConnectionInterval, row.Trimmingcut, row.EndRotation, row.ZAxisLength, row.BlowAirTime, row.VoltageRead,
                  row.CurrentCalculate, row.CurrentRead, row.CurrentDelta, row.PreConnectionCurrentRead, row.MovementSet,
                  row.MovementRead, row.MovementDelta, row.ForceSet, row.ForceRead, row.ForceDelta, row.PreConnectionForceRead,
                  row.ConnectionNumber, row.CycleNumber, row.CyclePosition, row.StatorCode_BarCode, row.Quality) 
       conn.commit()
       
    except Exception as Argument: 
          filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S") 
          f = open("I:\Arbeitsgruppen\Traceability\Sutoda-Ibraham\Fehlerbehandlung\schweissanlage" + filename1+ ".txt", "a")
          f.write(f'{filename}\n')
          f.write(str(Argument)) 
          f.write(f'\n{row}\n')
          f.close()

conn.close()