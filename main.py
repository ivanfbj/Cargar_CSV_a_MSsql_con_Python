import pyodbc
import pandas as pd
from tqdm import tqdm
import time
import os

# # Create in Microsoft SQL Server
# CREATE TABLE [dbo].[DepartmentTest](
# 	[DepartmentID] [int] NOT NULL,
# 	[Name] [nvarchar](100) NOT NULL,
# 	[GroupName] [nvarchar](100) NOT NULL
# ) ON [PRIMARY]
# GO

# CREATE TABLE [dbo].[DepartmentTest_bulkInsert](
# 	[DepartmentID] [int] NOT NULL,
# 	[Name] [nvarchar](100) NOT NULL,
# 	[GroupName] [nvarchar](100) NOT NULL
# ) ON [PRIMARY]
# GO

if __name__ == '__main__':

    name_file = 'department_pipe_line_masivo.csv'
    file_abspath = os.path.abspath(name_file)

    # insert data from csv file into dataframe.
    df = pd.read_csv(name_file, sep='|')
    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    driver = 'SQL Server'
    server = 'AGN5\SQLEXPRESS'  # select @@SERVERNAME
    database = 'Test_Local'
    username = 'usertest'
    password = '123456789'

    connection = pyodbc.connect(
        f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    cursor = connection.cursor()

    # truncate table
    cursor.execute('TRUNCATE TABLE DepartmentTest')
    cursor.execute('TRUNCATE TABLE DepartmentTest_bulkInsert')

    # Insert Dataframe into SQL Server:
    start_time_insert = time.strftime("%H:%M:%S")
    print('Hora inicio Insert por medio de un ciclo:\t', start_time_insert)

    for index, row in tqdm(df.iterrows()):
        cursor.execute('INSERT INTO DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)',
                       row.DepartmentID, row.Name, row.GroupName)
    connection.commit()

    end_time_insert = time.strftime("%H:%M:%S")
    print('Hora fin Insert por medio de un ciclo:\t\t', end_time_insert)

    print('##########################################################################')
    start_time_bulk = time.strftime("%H:%M:%S")
    print('Hora inicio bulk:\t', start_time_bulk)

    cursor.execute(
        f"BULK INSERT DepartmentTest_bulkInsert FROM '{file_abspath}' WITH(FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRSTROW=2)")
    connection.commit()
    
    end_time_bulk = time.strftime("%H:%M:%S")
    print('Hora fin bulk:\t\t', end_time_bulk)

    cursor.close()
    connection.close()
