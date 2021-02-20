import psycopg2

def create_conn(user: str, pwd: str, db: str):
    try:
        conn = psycopg2.connect(f"host=localhost dbname={db} user={user} password={pwd}")
    except:
        print("Unable to connect to database.")
        raise
    else:
        return conn


def execute_sql(sql: str, db: str, user: str, pwd: str):
    conn = create_conn(db, user, pwd)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except:
        raise
    else:
        print("SQL successfuly executed.")
    finally:
        conn.close()

def main():

    # Set parameters
    username = "henrique"
    password = "123456"
    db_name = "grupo_e"

    create_tables_sql = """CREATE TABLE cidade(
        cod_ibge bigint not null,
        nome varchar,
        populacao bigint,
        estado varchar(2),
        CONSTRAINT cidade_pk PRIMARY KEY(cod_ibge)
    );

    CREATE TABLE registro_covid(
        data date,
        cod_ibge bigint,
        last_available_confirmed bigint,
        last_available_deaths bigint,
        epidemiological_week int,
        new_confirmed bigint,
        new_deaths bigint,
        last_available_date date,
        last_available_death_rate real,
        CONSTRAINT registro_covid_pk PRIMARY KEY(data, cod_ibge),
        CONSTRAINT cidade_fk FOREIGN KEY (cod_ibge) references cidade (cod_ibge)
    );"""

    execute_sql(create_tables_sql, username, password, db_name)

if __name__ == "__main__":
    main()
