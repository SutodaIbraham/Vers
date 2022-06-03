# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 15:15:01 2022

@author: ibraham
"""
import glob
import pandas as pd
import pandas.io.sql
import pyodbc
from datetime import datetime
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Meier_Impraegnieranlage\Datensicherung MMC HMI Panel_08.04.2021\SAVELOG/*.csv")
for filename in filenames:
  try:
    p = pd.read_csv(filename, sep=";" )
    
   
    p['VarValue']=p['VarValue'].str.replace(',','.')
    p['Time_ms']=p['Time_ms'].str.replace(',','.')
    p['VarValue'] = pd.to_numeric(p['VarValue'], errors = 'ignore')
    p['Time_ms'] = pd.to_numeric(p['Time_ms'], errors = 'ignore')
    
    for row in p.itertuples():
      cursor.execute('''
                INSERT INTO dbo.Impreagnieranalge (Varname,TimeString,VarValue,Validity,Time_ms)
                VALUES (?,?,?,?,?)''',
                row.VarName, row.TimeString, row.VarValue, row.Validity, row.Time_ms)

      conn.commit()
  except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Meier_Impraegnieranlage + filename1+ ".txt", "a+")
            f.write(f'{filename}\n')
            f.write(str(Argument))
            f.write(f'\n{row}\n')
            f.close()
conn.close()