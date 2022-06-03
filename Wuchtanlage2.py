# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 12:49:21 2022

@author: ibraham
"""


import glob
import pandas as pd
import pandas.io.sql
import pyodbc
import datetime
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()


filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Wuchtanlage_Schenck\OUT-Dateien/*.csv")
for filename in filenames:
 try:
  p = pd.read_csv(filename)

  p.columns = ['Time1','Run','Station', 'Type1','Rotor_ID','Tolerance','Speed_rpm', 
      'Amount_1', 'Angle_1', 'Amount_2', 'Angle_2', 'Amount_3', 'Angle_3']

  p = p.fillna(0)

  for row in p.itertuples():
      cursor.execute('''
                INSERT INTO dbo.Wuchtanlage(Time1,Run, Station, Type1, Rotor_ID,Tolerance,
                Speed_rpm, Amount_1, Angle_1, Amount_2, Angle_2, Amount_3, Angle_3 )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ''',
                row.Time1, row.Run, row.Station, row.Type1, row.Rotor_ID, row.Tolerance, row.Speed_rpm,
                row.Amount_1, row.Angle_1, row.Amount_2, row.Angle_2, row.Amount_3, row.Angle_3 )
      conn.commit()
 except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Wuchtanlage_Schenck + filename1+ ".txt", "a")
            f.write(f'{filename}\n')
            f.write(str(Argument))
            f.write(f'\n{row}\n')
            f.close()
conn.close()