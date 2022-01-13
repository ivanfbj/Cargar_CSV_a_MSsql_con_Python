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

    # Se estable la conexión a la base de datos con sus respectivos valores del driver y servidor, la base de datos que se trabajará es la "master"
    # Se implemente el try exception en caso de error en las conexiones a la base de datos
    try:
        connection_windows_authentication = pyodbc.connect(
            f'DRIVER={driver_name};SERVER={server_name};DATABASE=master;trusted_connection=true',autocommit=True)
        print('Conexión exitosa a la base de datos.')

        cursor = connection_windows_authentication.cursor()
        # Consulta para identificar si existe o no la base de datos que se necesita.
        cursor.execute(f'''SELECT name FROM sysdatabases WHERE (name = '{database_name}')''')
        row_database = cursor.fetchone()
        # SI la base de datos NO EXISTE, se ejecuta el comando que crea la base de datos que se necesita.
        if row_database is None:
            print(f'NO EXISTE la base de datos "{database_name}".')
            cursor.execute(f'CREATE DATABASE {database_name};')
            # connection.commit() # No se requiere esta linea ya que en la variable 'connection_windows_authentication' se está utilizando el autocommit
            print(f'la base de datos "{database_name}" a sido creada exitosamente.')
        # SI la base de datos YA EXISTE se muestra el mensaje.
        if row_database is not None:
            print(f'La base de datos "{database_name}" ya existe.')

        # Consulta para identificar si existe o no el USUARIO que se necesita en de la base de datos.
        cursor.execute(f'''SELECT name FROM syslogins WHERE name ='{user_name}';''')
        row_user_in_master = cursor.fetchone()
        # SI el usuario NO EXISTE se crea sobre la base de datos master
        # Se le asigna al usuario el ROL de bulkadmin para que pueda insertar archivos planos a las tablas en base de datos.
        # Sobre la base de datos que se ha creado y se va a trabajar se crea el usuario y se asigna el rol db_owner, para que quede como propietario.
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
        # SI el usuario YA EXISTE en la base de datos master se muestra el mensaje.
        if row_user_in_master is not None:
            print('Usuario ya existe')

    except Exception as ex:
        print('Error en la ejecución del TRY de creación de la base de datos y usuario:\n', ex)


