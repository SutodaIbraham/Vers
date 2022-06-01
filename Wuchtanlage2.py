# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 12:49:21 2022

@author: ibraham
"""

import glob
import pandas as pd
import pandas.io.sql
import pyodbc



filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Wuchtanlage_Schenck\OUT-Dateien/*.csv")
dfs = []
for filename in filenames:
  dfs.append(pd.read_csv(filename))
big_frame = pd.concat(dfs, ignore_index=True)
big_frame.columns = ['Time1','Run','Station', 'Type1','Rotor_ID','Tolerance','Speed_rpm', 
      'Amount_1', 'Angle_1', 'Amount_2', 'Angle_2', 'Amount_3', 'Angle_3']

big_frame = big_frame.fillna(0)


server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()

for row in big_frame.itertuples():
    try:
      cursor.execute('''
                INSERT INTO dbo.Wuchtanlage(Time1,Run, Station, Type1, Rotor_ID,Tolerance,
                Speed_rpm, Amount_1, Angle_1, Amount_2, Angle_2, Amount_3, Angle_3 )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ''',
                row.Time1, row.Run, row.Station, row.Type1, row.Rotor_ID, row.Tolerance, row.Speed_rpm,
                row.Amount_1, row.Angle_1, row.Amount_2, row.Angle_2, row.Amount_3, row.Angle_3 )
      conn.commit()
    except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open("I:\Arbeitsgruppen\Traceability\Fehlerbehandlung\Wuchtanlage" + filename1+ ".txt", "a")
            f.write(str(Argument))
            f.write(f'{row}\n')
            f.close()
conn.close()

'''
big_frame['Amount_1'] = pd.to_numeric(big_frame['Amount_1'].astype(str).str.replace(',', '.'), downcast='float')
big_frame['Amount_2'] = pd.to_numeric(big_frame['Amount_2'].astype(str).str.replace(',', '.'), downcast='float')
#big_frame['Amount_3'] = pd.to_numeric(big_frame['Amount_3'].astype(str).str.replace(',', '.'), downcast='float')
big_frame['Angle_1'] = pd.to_numeric(big_frame['Angle_1'].astype(str).str.replace(',', '.'), downcast='float')
big_frame['Angle_2'] = pd.to_numeric(big_frame['Angle_2'].astype(str).str.replace(',', '.'), downcast='float')
#big_frame['Angle_3'] = pd.to_numeric(big_frame['Angle_3'].astype(str).str.replace(',', '.'), downcast='float')
big_frame['Speed_rpm'] = pd.to_numeric(big_frame['Speed_rpm'], downcast='float')
big_frame['Station'] = pd.to_numeric(big_frame['Station'], downcast='float')
big_frame['Run'] = pd.to_numeric(big_frame['Run'], downcast='float')
#big_frame.drop(big_frame.index[[14]])
#print(big_frame.iloc[13:17])i
#df.loc[df[col_name] == float('-inf'), col_name] = -1
#df.loc[df[col_name] == float('inf'), col_name] = 1

'''