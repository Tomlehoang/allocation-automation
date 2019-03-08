dev_Database = {
    'host': 'pgresqljiradev.dai.netdai.com',
    'database': 'allocation',
    'port' : '5432',
    'confluenceUrl': 'https://confluencedev1.daicompanies.com',
    'jiraUrl': {'server': 'https://jiradev1.daicompanies.com/'}
}

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 10)
#pd.set_option('display.width', 1000)

DbName = 'tom.le'
DbPass = 'Whatever1!'
#enviroment Setup
def envi_setup(enviDB):
    global host
    global database
    global port
    global jiraUrl
    global confluenceUrl
    host = enviDB['host']
    database = enviDB['database']
    port = enviDB['port']
    confluenceUrl = enviDB['confluenceUrl']
    jiraUrl = enviDB['jiraUrl']

#choose enviroment prod_Database or dev_Database
envi_setup(dev_Database)
mydb = psycopg2.connect(user = DbName,
            password = DbPass,
            host = host,
            port = port,
            database = database)
mycursor = mydb.cursor()

engine = create_engine('postgresql://tom.le:Whatever1!@pgresqljiradev:5432/allocation')

def dai_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.0710
    else:
        if(row.company == 'DAI'):
            return 1
        else:
            return 0

def won_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.0440
    else:
        if(row.company == 'WON'):
            return 1
        else:
            return 0

def ons_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.0227
    else:
        if(row.company == 'ONS'):
            return 1
        else:
            return 0

def ocm_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.0142
    else:
        if(row.company == 'OCM'):
            return 1
        else:
            return 0

def ibd_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.1548
    else:
        if(row.company == 'IBD'):
            return 1
        else:
            return 0

def ods_allocation(row):
    if(row['Overall Department'] == 'Shared Services'):
        return 0.6932
    else:
        if(row.company == 'ODS'):
            return 1
        else:
            return 0

"""f=pd.read_csv("E1.csv")
f['E1'] = '1'
f['E3'] = '0'
keep_col = ['Name','UserPrincipalName','Department','company','E1','E3']
new_f = f[keep_col]
new_f.to_csv("newE1File.csv", index=True)

f=pd.read_csv("E3.csv")
f['E1'] = '0'
f['E3'] = '1'
keep_col = ['Name','UserPrincipalName','Department','company','E1','E3']
new_f = f[keep_col]
#with open('newFile.csv', 'a') as f:
new_f.to_csv("newE3File.csv", index=True)


with open('newE1File.csv', 'r') as f:
    next(f)  # Skip the header row.
    mycursor.copy_from(f, 'E1license', sep=',')
mydb.commit()

with open('newE3File.csv', 'r') as f:
    next(f)  # Skip the header row.
    mycursor.copy_from(f, 'E3license', sep=',')
mydb.commit()"""

df = pd.read_sql_query('select * from e1license',con=engine)
df3 = pd.read_sql_query('select * from e3license',con=engine)
df = df.append(df3)
df = df.sort_values(by=['fullname'], axis = 0)
conditions = [
    (df['department'] == 'Reception'),
    (df['department'] == 'Advertising Sales'),
    (df['department'] == 'Research and Development'),
    (df['department'] == 'Finishing'),
    (df['department'] == 'Compliance'),
    (df['department'] == 'Customer Support'),
    (df['department'] == 'Equity Sales Trading'),
    (df['department'] == 'Portfolio Management'),
    (df['department'] == 'Quantitative Services'),
    (df['department'] == 'Trading Operations'),
    (df['department'] == "O'Neil Securities, Inc"),
    (df['department'] == 'Research'),
    (df['department'] == 'Security'),
    (df['department'] == ''),
    (df['department'] == 'Video & Business Partneresh'),
    (df['department'] == 'Video & Business Developement'),
    (df['department'] == 'Video and Business Partnersh'),
    (df['department'] == 'Production Control'),
    (df['department'] == 'Prepress'),
    (df['department'] == 'Digital Press'),
    (df['department'] == 'ODS - Digital Print'),
    (df['department'] == 'Software Engineering'),
    (df['department'] == 'Quality Assurance Engineering'),
    (df['department'] == 'ODS Programming TX'),
    (df['department'] == 'Network Operations Center (NOC)'),
    (df['department'] == 'Desktop'),
    (df['department'] == 'Database Engineer'),
    (df['department'] == 'Telecom'),
    (df['department'] == 'System Engineering'),
    (df['department'] == 'Programming Engineering - Panaray')
]
choices = [
    'Administration',
    'Advertising',
    'Allocable - All Depts',
    'Fulfillment',
    'Human Resources',
    'Main',
    'Main',
    'Main',
    'Main',
    'Main',
    'Main',
    'Main',
    'Main',
    'Main1',
    'Partnership & Video Strategy',
    'Partnership & Video Strategy',
    'Partnership & Video Strategy',
    'Production',
    'Production',
    'Production',
    'Production',
    'Technology',
    'Technology',
    'Technology',
    'Shared Services',
    'Technology',
    'Shared Services',
    'Technology',
    'Shared Services',
    'Technology'
]

licCon = [
    (df['e1'] == 1),
    (df['e3'] == 1)
]
licChoi = [
    'Office 365 Enterprise E1',
    'Office 365 Enterprise E3'
]
writer = pd.ExcelWriter("output.xlsx", engine='xlsxwriter')
df.insert(4, 'Overall Department', np.select(conditions, choices, default=df['department']))
df.drop(columns='user_id', axis=1, inplace=True)
#df.drop(df.columns[1], axis=1, inplace=True)
df['Licenses'] = np.select(licCon, licChoi, default='N/A')
df.loc[:, 'DAI'] = df.apply(dai_allocation, axis = 1)
df.loc[:, 'WON'] = df.apply(won_allocation, axis = 1)
df.loc[:, 'ONS'] = df.apply(ons_allocation, axis = 1)
df.loc[:, 'OCM'] = df.apply(ocm_allocation, axis = 1)
df.loc[:, 'IBD'] = df.apply(ibd_allocation, axis = 1)
df.loc[:, 'ODS'] = df.apply(ods_allocation, axis = 1)
df.to_excel(writer, sheet_name='Name', index=False)
df = df.fillna(0)
table = pd.pivot_table(df,
       index=['Overall Department'],
       columns='Licenses',
       margins=True,
       values=['DAI','WON', 'ONS', 'OCM', 'IBD','ODS'],
       aggfunc=sum,
       fill_value=0
       )

table.to_excel(writer, sheet_name='Percentage')
#workbook  = writer.book
#worksheet = writer.sheets['Percentage']
#format = workbook.add_format({'num_format': '0.00%'})
#worksheet.set_column('B:I', None, format)
table = pd.pivot_table(df,
       index=['Overall Department'],
       columns='company',
       margins=True,
       values=['e1','e3'],
       aggfunc=np.sum,
       fill_value=0
       )
table.to_excel(writer, sheet_name='Count')
writer.save()
