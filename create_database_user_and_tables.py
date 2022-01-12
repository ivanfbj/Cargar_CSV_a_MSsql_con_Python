import pyodbc as pyodbc

if __name__ == '__main__':

        # Se declaran las variables que permitirán la conexión con la base de datos
        # Some other example server values are
        # server_name = 'localhost\sqlexpress' # for a named instance
        # server_name = 'myserver,port' # to specify an alternate port
        driver_name = 'SQL Server'
        server_name = 'AGN5\SQLEXPRESS'  # select @@SERVERNAME
        database_name = 'TESTING_PYTHON'
        user_name = 'usertest'
        password = '123456789'

        # Se estable la conexión a la base de datos con sus respectivo valores anteriormente declarados
        # Se implemente el try exception en caso de error en la conexión
        try:
                connection_master = pyodbc.connect(
                        # f'DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}',autocommit=True)
                        f'DRIVER={driver_name};SERVER={server_name};DATABASE=master;trusted_connection=true',autocommit=True)
                print('Conexión exitosa a la base de datos.')

                cursor = connection_master.cursor()
                # cursor.execute('select 0/0')
                # row = cursor.fetchone()
                # print(row)
                cursor.execute(f'''SELECT name FROM sysdatabases WHERE (name = '{database_name}')''')
                row = cursor.fetchone()
                print(row)
                if row is None:
                        print('la base de datos NO EXISTE.')
                        cursor.execute(f'CREATE DATABASE {database_name};')
                        # connection.commit() # No se requiere esta linea ya que en la variable 'connection_master' se está utilizando el autocommit
                        print(f'la base de datos "{database_name}" a sido creada exitosamente')
                if row is not None:
                        print(f'La base de datos "{database_name}" ya existe.')



        except Exception as ex:
                print('Error en la ejecución del TRY:\n',ex)


