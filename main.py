import pyodbc
import pandas as pd
from tqdm import tqdm

# # Create in Microsoft SQL Server
# CREATE TABLE [dbo].[DepartmentTest](
# 	[DepartmentID] [smallint] NOT NULL,
# 	[Name] [nvarchar](100) NOT NULL,
# 	[GroupName] [nvarchar](100) NOT NULL
# ) ON [PRIMARY]
# GO


if __name__ == '__main__':

    # insert data from csv file into dataframe.
    df = pd.read_csv("department_pipe_line.csv", sep='|')
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
    cursor.execute("TRUNCATE TABLE DepartmentTest")

    # Insert Dataframe into SQL Server:
    for index, row in tqdm(df.iterrows()):
        cursor.execute("INSERT INTO DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)",
                       row.DepartmentID, row.Name, row.GroupName)
    connection.commit()
    cursor.close()
