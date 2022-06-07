# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 08:31:39 2022

@author: ibraham
"""


import glob
import os
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
filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Meier_Impraegnieranlage\Statorplatz 01\_PROZESSDATEN_Gesammelt/*.csv")
filenames.sort(key=os.path.getctime)

try:
 for filename in filenames:
    p = pd.read_csv(filename, sep=";" )
    p['Materialnummer'] = os.path.basename(filename).split(".")[0].split("_")[0]
    p['Datum'] = pd.to_datetime( os.path.basename(filename).split(".")[0].partition("_")[2], format = '%d_%m_%Y')
    
    p['Solltemperatur_Stromwärme'] = pd.to_numeric(p['Solltemperatur_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
    p['Isttemperatur_Stromwärme'] = pd.to_numeric(p['Isttemperatur_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
    p['Iststrom_Stromwärme'] = pd.to_numeric(p['Iststrom_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
    p['Isttemperatur_Ofen'] = pd.to_numeric(p['Isttemperatur_Ofen'] .astype(str).str.replace(',', '.'), downcast='float')

 for row in p.itertuples():
    cursor.execute('''INSERT INTO dbo.Impreagnieranlage_Prozessdaten (Solltemperatur_Stromwärme, Isttemperatur_Stromwärme, Iststrom_Stromwärme, Isttemperatur_Ofen, Materialnummer, 
                   Datum)VALUES (?, ?, ?, ?, ?, ?)''', row.Solltemperatur_Stromwärme, row.Isttemperatur_Stromwärme,  row.Iststrom_Stromwärme, 
                                                      row.Isttemperatur_Ofen,  row.Materialnummer,  row.Datum)

    conn.commit()
except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Impreagnieranlage_Prozessdaten + filename1+ ".txt", "a")
            f.write(f'{filename}\n')
            f.write(str(Argument))
            f.write(f'\n{row}\n')
            f.close()

conn.close()      
