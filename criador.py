import psycopg2
import sys

#This .py connects to the localhost postgresql and creates a new database "grupo_e" that will be used to create tables
#Set your db configs on main function

def connect_db(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        print('%s database...' % params_dic.get("database"), end='')
        conn = psycopg2.connect(**params_dic)
        conn.autocommit = True
    except (Exception) as error:
        print("Unable to connect ", error)
        sys.exit(1)
    print("Connection successful!")
    return conn, conn.cursor()


def execute_sql(conn, cursor, sql_list):
    try:
        for sql in sql_list:
            cursor.execute(sql)
    except (Exception) as error:
        print(error)
    finally:
        conn.close()
        cursor.close()


def main():

    # Set your local variables here!
    params = dict(
        database="postgres",
        user="postgres",
        host="127.0.0.1",
        port="5432",
        password="root")

    sql_cidade = '''CREATE TABLE cidade(
    	cod_ibge bigint not null,
    	nome varchar,
    	populacao bigint,
    	estado varchar(2),
    	CONSTRAINT cidade_pk PRIMARY KEY(cod_ibge)
    )'''

    sql_covid = '''create table registro_covid(
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

    #Connection postgresql
    conn, cursor = connect_db(params)

    sql_list = list()

    #Create database grupo_e
    sql_list.append('''DROP DATABASE IF EXISTS grupo_e''')
    sql_list.append('''CREATE database grupo_e''')
    execute_sql(conn, cursor, sql_list)
    print("Database grupo_e created successfully!")

    # Connect database grupo_e
    params.update(database="grupo_e")
    conn, cursor = connect_db(params)

    #Create tables
    sql_list.clear()
    sql_list.append("DROP TABLE IF EXISTS registro_covid CASCADE")
    sql_list.append("DROP TABLE IF EXISTS cidade CASCADE")
    sql_list.append(sql_cidade)
    sql_list.append(sql_covid)
    execute_sql(conn, cursor, sql_list)
    print("Tables created successfully!")


if __name__ == "__main__":
    main()



