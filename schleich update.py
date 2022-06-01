# -*- coding: utf-8 -*-
"""
Created on Wed May 25 06:57:03 2022

@author: ibraham
"""
import glob
import os
import pandas as pd
import pyodbc
import datetime
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
pd.set_option('display.max_columns', 500)

filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Schleich-EOL\CSV/*.csv")
filenames.sort(key=os.path.getctime)
Spalten = [ 'Text1', 'Ist', 'Soll',	'Minimum',	'Maximum',	'Einheit',	'IO_NIO']
try:
  for filename in filenames: 
      p = pd.read_csv(filename,sep = '\t', encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(';', expand=True)
      
      if len(p.columns)<7:
          p = pd.read_csv(filename,sep = '\t', encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(',', expand=True)
          if len(p.columns)>=8: 
             p.drop(p.iloc[:, 7:len(p.columns)], inplace = True, axis = 1)
          
      if len(p.columns)>=8: 
         p.drop(p.iloc[:, 7:len(p.columns)], inplace = True, axis = 1) 
      
      p.columns = Spalten
      for i in range(len(p)):
          p['SerNr'] = p['Ist'][0]
          p['TestPlan'] = p['Ist'][1]
          p['Inspector'] = p['Ist'][2] 
          p['Datum'] = p['Ist'][3]
          p['Zeit'] = p['Ist'][4]
          p['OrderNr'] = p['Ist'][5]
          p['Result'] = p['Ist'][6]
      
      
      p.drop(p.head(8).index, inplace=True)   
      p['Ist_']= p['Ist'].str.split('@').str[1].astype(str).str.split('°C').str[1]
      p.loc[p['Ist_'].isna(), 'Ist_'] =  p['Ist'] 
      p['L1'] = p['Ist'].str.split('=').str[1].astype(str).str.split(' ').str[0].fillna(0)
      p['L2'] = p['Ist'].str.split('=').str[2].astype(str).str.split(' ').str[0].fillna(0)
      p['L3'] = p['Ist'].str.split('=').str[3].astype(str).str.split(' ').str[0].fillna(0)
      p = p.replace(['[Soll]', '[Ist]', '[Min]', '[Text]', '[Max]'], value = 0)
      p['Soll'] = p['Soll'].str.strip('"')
      p['Minimum'] = p['Minimum'].str.strip('"')
      p['Maximum'] = p['Maximum'].str.strip('"')
      p['Ist_'] = p['Ist_'].str.strip('"')
      p['Soll'] = pd.to_numeric(p['Soll']).fillna(0)
      p['Minimum'] = pd.to_numeric(p['Minimum']).fillna(0)
      p['Maximum'] = pd.to_numeric(p['Maximum']).fillna(0)
      p['Datum'] = pd.to_datetime(p['Datum'],infer_datetime_format=True).dt.strftime('%d/%m/%y')
      p['Zeit'] = pd.to_datetime(p['Zeit'],infer_datetime_format=True).dt.strftime('%H:%M:%S')
      p['Ist_'] = pd.to_numeric(p['Ist_']).fillna(0)
      p['L1'] = pd.to_numeric(p['L1'], errors='coerce' ).fillna(0)
      p['L2'] = pd.to_numeric(p['L2'], errors='coerce').fillna(0)
      p['L3'] = pd.to_numeric(p['L3'], errors='coerce' ).fillna(0)

      
           
      for row in p.itertuples():
          cursor.execute('''
                    INSERT INTO dbo.Schleich2(
                    Text1,  Ist, Soll, Minimum, Maximum, Einheit, IO_NIO, SerNr, TestPlan, Inspector, Datum, Zeit, OrderNr, Result, Ist_, L1, L2, L3)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)''',
                    row.Text1, row.Ist, row.Soll, row.Minimum, row.Maximum, row.Einheit,
                    row.IO_NIO, row.SerNr, row.TestPlan, row.Inspector, row.Datum,
                    row.Zeit, row.OrderNr, row.Result, row.Ist_, row.L1, row.L2, row.L3)
          conn.commit()
except Exception as Argument:
          filename1 = datetime.date.today() # filename1 erfasst das aktuelle Datum und die Uhrzeit 
          f = open(V.Schleich_EOL + filename1+ ".txt", "a") # f ist der Pfad von einer Textdatei
          f.write(str(Argument)) # in f wird die Uhrsache des Exceptions (das Argument) geschrieben
          f.write(f'{row}\n')# in f wird die Zeile die das Exception verursacht geschrieben
          f.close()
conn.close()
     
   
    