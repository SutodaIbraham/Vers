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

filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Endmontage_Hoering\Speicherkarte 21042021/*csv")
for filename in filenames:
  try:
    p= pd.read_csv(filename, encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(';', expand=True) # sep='\n',
    # wenn Spaltennamen explizit übergeben werden, ist das Verhalten identisch mit header=None
    a = []; b = []; c = []; d = []; e = []; f = []
    # um aus p eine brauchbare Tabelle zu erstellen, werden die Elemente einzeln herausgeholt und am richtigen Platz platziert
    
    for i in range(0,6):
        a.append(p.iloc[0][0])# aus der Zelle 0-0 (Zeile 0 Spalte 0) wird die Liste a der Länge 5 gebastelt
        b.append(p.iloc[1][0])
        c.append(p.iloc[2][0])
        d.append(p.iloc[3][0])
        e.append(p.iloc[4][0])
        f.append(p.iloc[5][0])
    g = [p.iloc[7][0], p.iloc[8][0], p.iloc[9][0], p.iloc[10][0], p.iloc[11][0]] # Die g Liste besteht aus der Zeile 0 werden die Spalten 7,8,9,10 und 11 der Liste 
    h = [p.iloc[7][1], p.iloc[8][1], p.iloc[9][1], p.iloc[10][1], p.iloc[11][1]] 
    j = [p.iloc[7][2], p.iloc[8][2], p.iloc[9][2], p.iloc[10][2], p.iloc[11][2]]
    l = [0, 0, p.iloc[13][1],p.iloc[14][1], 0] # Zu der Liste l wird am Anfang und am Ende Nullen hinzugefügt, damit die Reihenfolge und Platzierung stimmt
    m = [0, 0, p.iloc[13][2],p.iloc[14][2], 0]
    n = [0, 0, p.iloc[13][3],p.iloc[14][3], 0]
    o = [0, 0, p.iloc[13][4],p.iloc[14][4], 0]
    
    Columns = ['Bezeichnung', 'Materialnummer', 'Seriennummer', 'Typ', 'Laenge', 'Bearbeiter','Uhrzeit', 'material_nr',
           'serien_nr','Weg', 'Kraft_bei_Abschaltung', 'Auswertung_Weg_Ok','Auswertung_Kontrollpunkt_Ok']
    df = pd.DataFrame(list(zip(g,a,b,c,d,e,f,h,j,l,m,n,o)), columns = Columns)
    
    df.drop(df.loc[df['Uhrzeit']=='LogValX'].index, inplace=True)
    df['Materialnummer'] = df['Materialnummer'].map(lambda x: x.lstrip('Materialnummer:'))
    df['Seriennummer'] = df['Seriennummer'].map(lambda x: x.lstrip('Seriennummer:'))
    df['Typ'] = df['Typ'].map(lambda x: x.lstrip('Typ:'))
    df['Laenge'] = df['Laenge'].map(lambda x: x.lstrip('Länge:'))
    df['Laenge'] = pd.to_numeric(df['Laenge'],errors='ignore')
    df['Bearbeiter'] = df['Bearbeiter'].map(lambda x: x.lstrip('Bearbeiter:'))
    df['Uhrzeit'] = df['Uhrzeit'].map(lambda x: x.lstrip(' Erstellungszeitpunkt: '))
    df['AM_PM'] = df['Uhrzeit'].astype(str).str.split(" ").str[3]
    df['Weg'] = pd.to_numeric(df['Weg'], errors = 'ignore')
    df['Kraft_bei_Abschaltung'] = pd.to_numeric(df['Kraft_bei_Abschaltung'], errors = 'ignore')
    df['Uhrzeit'] = df['Uhrzeit'].map(lambda x: x.rstrip(' AM '))
    df['Uhrzeit'] = df['Uhrzeit'].map(lambda x: x.rstrip(' PM '))
    df['Uhrzeit'] = pd.to_datetime(df['Uhrzeit'], errors = 'ignore') 
    df = df.fillna(0)

    for row in df.itertuples():
        cursor.execute('''
                  INSERT INTO dbo.Endmontage(
                  Bezeichnung, Materialnummer, Seriennummer, Typ, Laenge, Bearbeiter, Uhrzeit, material_nr,
                     serien_nr,Weg, Kraft_bei_Abschaltung, Auswertung_Weg_Ok,Auswertung_Kontrollpunkt_Ok, AM_PM)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Bezeichnung, 
                  row.Materialnummer, 
                  row.Seriennummer, 
                  row.Typ, 
                  row.Laenge,
                  row.Bearbeiter,
                  row.Uhrzeit,
                  row.material_nr,
                  row.serien_nr,
                  row.Weg,
                  row.Kraft_bei_Abschaltung,
                  row.Auswertung_Weg_Ok,
                  row.Auswertung_Kontrollpunkt_Ok,
                  row.AM_PM)

        conn.commit()
  except Exception as Argument:
          filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
          f = open(V.Endmontage_Hoering + filename1+ ".txt", "a+")
          f.write(f'{filename}')
          f.write(str(Argument))
          f.write(f'{row}')
          f.close()

 

conn.close()