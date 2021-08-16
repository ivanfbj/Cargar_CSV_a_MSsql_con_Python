import pyodbc
import pandas as pd
from tqdm import tqdm
import time
import os

# # Sentencias para crear las tablas directamente en Microsoft SQL Server y poder llevar a cabo la ejecución del código
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

    # Se declaran las variables para almacenar el nombre del archivo que se va a utilizar y el ruta completa donde se encuentra almacenado
    name_file = 'department_pipe_line_masivo.csv'
    file_abspath = os.path.abspath(name_file)

    # insert data from csv file into dataframe.
    df = pd.read_csv(name_file, sep='|')

    # Se declaran las variables que permitirán la conexión con la base de datos
    # Some other example server values are
    # server_name = 'localhost\sqlexpress' # for a named instance
    # server_name = 'myserver,port' # to specify an alternate port
    driver_name = 'SQL Server'
    server_name = 'AGN5\SQLEXPRESS'  # select @@SERVERNAME
    database_name = 'Test_Local'
    user_name = 'usertest'
    password = '123456789'

    # Se estable la conexión a la base de datos con sus respectivo valores anteriormente declarados
    connection = pyodbc.connect(
        f'DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}')

    cursor = connection.cursor()

    # Se borran las tablas completamente para simpre insertar información sobre las tablas vacías.
    cursor.execute('TRUNCATE TABLE DepartmentTest')
    cursor.execute('TRUNCATE TABLE DepartmentTest_bulkInsert')

    # Se captura la Hora de inicio de la ejecución
    start_time_insert = time.strftime("%H:%M:%S")
    print('Hora inicio Insert por medio de un ciclo:\t', start_time_insert)

    # Este es el ciclo que se encarga de recorrer cada registro del archivo para generar el INSERT por cada registro a la base de datos
    # Insert Dataframe into SQL Server:
    for index, row in tqdm(df.iterrows()):
        cursor.execute('INSERT INTO DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)',
                       row.DepartmentID, row.Name, row.GroupName)
    connection.commit()

    # Se captura la Hora de fin de la ejecución
    end_time_insert = time.strftime("%H:%M:%S")
    print('Hora fin Insert por medio de un ciclo:\t\t', end_time_insert)

    print('##########################################################################')

    # Se captura la Hora de inicio de la ejecución
    start_time_bulk = time.strftime("%H:%M:%S")
    print('Hora inicio bulk:\t', start_time_bulk)

    # Sentencia que ejecutael BULK Insert del archivo a la base de datos.
    cursor.execute(
        f"BULK INSERT DepartmentTest_bulkInsert FROM '{file_abspath}' WITH(FIELDTERMINATOR='|', ROWTERMINATOR='\n', FIRSTROW=2)")
    connection.commit()

    # Se captura la Hora de fin de la ejecución
    end_time_bulk = time.strftime("%H:%M:%S")
    print('Hora fin bulk:\t\t', end_time_bulk)

    cursor.close()
    connection.close()
