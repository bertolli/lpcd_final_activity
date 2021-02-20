import psycopg2

#Set your variables here!
your_db = "postgres"
your_user = "postgres"
your_host = "127.0.0.1"
your_port = "5432"
your_password = "root"

#establishing the connection
conn = psycopg2.connect(database=your_db, user=your_user, password=your_password, host=your_host, port= your_port)

#auto commit enable
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Create db
cursor.execute('''DROP DATABASE IF EXISTS grupo_e''')
sql_create = '''CREATE database grupo_e''';

cursor.execute(sql_create)
print("Base de dados grupo_e criada com sucesso!")
conn.close()
cursor.close()

#connecting on created db
conn = psycopg2.connect(database="grupo_e", user=your_user, password=your_password, host=your_host, port= your_port)
conn.autocommit = True
cursor = conn.cursor()


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
conn.commit()
print("Tabela cidade criada com sucesso!")

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
conn.commit()
print("Tabela registro_covid criada com sucesso!")


#Closing the connection
conn.close()
cursor.close()
print("Fechando a conexão")

