import psycopg2

#Set your local variables here!
params = dict(
    database = "postgres",
    user = "postgres",
    host = "127.0.0.1",
    port = "5432",
    password = "root")

def connect_db(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the %s database...' % params_dic.get("database"), end='')
        conn = psycopg2.connect(**params_dic)
        conn.autocommit = True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful!")
    return conn, conn.cursor()

#establishing the connection
conn, cursor = connect_db(params)

#Create db grupo_e
cursor.execute('''DROP DATABASE IF EXISTS grupo_e''')
sql_create = '''CREATE database grupo_e''';

cursor.execute(sql_create)
print("Database grupo_e created successfully!")
conn.close()
cursor.close()

#connecting on created db grupo_e
params.update(database="grupo_e")
conn,cursor = connect_db(params)

#Drop before create
cursor.execute("DROP TABLE IF EXISTS registro_covid")
cursor.execute("DROP TABLE IF EXISTS cidade")

#Creating table cidade
sql_cidade ='''CREATE TABLE cidade(
	cod_ibge bigint not null,
	nome varchar,
	populacao bigint,
	estado varchar(2),
	CONSTRAINT cidade_pk PRIMARY KEY(cod_ibge)
)'''
cursor.execute(sql_cidade)
print("Table cidade created successfully!")

#Creating table registro_covid
sql_covid ='''create table registro_covid(
	"data" date,
	cod_ibge bigint,
	last_available_confirmed bigint,
	last_available_deaths bigint,
	epidemiological_week int,
	new_confirmed bigint,
	new_deaths bigint,
	last_available_date date,
	last_available_death_rate real,
	CONSTRAINT registro_covid_pk PRIMARY KEY("data", cod_ibge),
	CONSTRAINT cidade_fk FOREIGN KEY (cod_ibge) references cidade (cod_ibge)
)'''
cursor.execute(sql_covid)
print("Table registro_covid created successfully!")

#Closing the connection
conn.close()
cursor.close()
print("Closing connection")




