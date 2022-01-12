import pyodbc as pyodbc

if __name__ == '__main__':

        # Se declaran las variables que permitirán la conexión con la base de datos
        # Some other example server values are
        # server_name = 'localhost\sqlexpress' # for a named instance
        # server_name = 'myserver,port' # to specify an alternate port
        driver_name = 'SQL Server'
        server_name = 'AGN5\SQLEXPRESS'  # select @@SERVERNAME
        database_name = 'TESTING_PYTHON'
        user_name = 'pythonSql'
        user_password = '123456789'

        # Se estable la conexión a la base de datos con sus respectivo valores anteriormente declarados
        # Se implemente el try exception en caso de error en la conexión
        try:
                connection_windows_authentication = pyodbc.connect(
                        # f'DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password}',autocommit=True)
                        f'DRIVER={driver_name};SERVER={server_name};DATABASE=master;trusted_connection=true',autocommit=True)
                print('Conexión exitosa a la base de datos.')

                cursor = connection_windows_authentication.cursor()
                # cursor.execute('select 0/0')
                # row = cursor.fetchone()
                # print(row)
                cursor.execute(f'''SELECT name FROM sysdatabases WHERE (name = '{database_name}')''')
                row_database = cursor.fetchone()
                # print(row)
                if row_database is None:
                        print(f'NO EXISTE la base de datos "{database_name}".')
                        cursor.execute(f'CREATE DATABASE {database_name};')
                        # connection.commit() # No se requiere esta linea ya que en la variable 'connection_windows_authentication' se está utilizando el autocommit
                        print(f'la base de datos "{database_name}" a sido creada exitosamente.')
                if row_database is not None:
                        print(f'La base de datos "{database_name}" ya existe.')

                cursor.execute(f'''SELECT name FROM syslogins WHERE name ='{user_name}';''')
                row_user_in_master = cursor.fetchone()
                if row_user_in_master is None:
                        print(f'NO EXISTE el Usuario "{user_name}".')
                        cursor.execute(f'''
                                CREATE LOGIN [{user_name}] WITH PASSWORD=N'{user_password}'
                                        , DEFAULT_DATABASE=[{database_name}]
                                        , DEFAULT_LANGUAGE=[Español]
                                        , CHECK_EXPIRATION=OFF
                                        , CHECK_POLICY=OFF
                        ''')
                        print(f'El usuario "{user_name}" a sido creado exitosamente en la base de datos master.')
                        cursor.execute(f'''ALTER SERVER ROLE [bulkadmin] ADD MEMBER [{user_name}]''')
                        print(f'El usuario "{user_name}" ahora cuenta con el ROL de bulkadmin.')
                        cursor.execute(f'''
                                USE {database_name}
                                CREATE USER {user_name} from login {user_name};
                                ALTER ROLE [db_owner] ADD MEMBER [{user_name}];
                        ''')
                        print(f'El usuario "{user_name}" a sido creado exitosamente en la base de datos "{database_name}" y se le ha asignado el rol "db_owner".')
                if row_user_in_master is not None:
                        print('Usuario ya existe')

        except Exception as ex:
                print('Error en la ejecución del TRY:\n', ex)


