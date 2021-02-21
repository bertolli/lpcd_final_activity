import pandas as pd
import psycopg2
import psycopg2.extras as extras

#Change your db variables on main function

#DB connection
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

#Returns new df with selected columns
def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame


#Inserts by a given DF
def execute_insert(cursor, datafrm, table):
    # Creates tuple
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # Identifies columns
    cols = ','.join(list(datafrm.columns))

    # Creates sql
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    try:
        extras.execute_values(cursor, sql, tpls)
        print("Data was successfully inserted into %s" % table)
    except (Exception) as err:
        print(err)
        cursor.close


def main():

    #Set your connection variables here
    params = dict(
        database="grupo_e",
        user="postgres",
        host="127.0.0.1",
        port="5432",
        password="root")

    #Set you .csv path here
    csv_file_path = "covid19_casos_brasil.csv"

    #Load DF
    full_df = pd.read_csv(csv_file_path)

    #City columns
    cols_city = ['city_ibge_code', 'city', 'estimated_population_2019', 'state']

    #Covid columns
    cols_covid = ['date', 'city_ibge_code', 'last_available_confirmed',
              'last_available_deaths', 'epidemiological_week', 'new_confirmed',
              'new_deaths', 'last_available_date', 'last_available_death_rate']

    #Filters chosen cities
    cities = ['Sorocaba', 'Ponta Grossa', 'Curitiba', 'Manaus', 'São Paulo']

    #Create new DF with chosen cities
    frames = list()
    for c in cities:
        frames.append(full_df[full_df['city'] == c])
        df_cities_covid = pd.concat(frames)

    #splits DF into cities and covid stats
    df_cities = select_columns(df_cities_covid,cols_city)
    df_covid = select_columns(df_cities_covid, cols_covid)

    #removes duplicates cities according ibge_cod
    df_cities = df_cities.drop_duplicates(subset=['city_ibge_code'])

    #renames columns according database dictionary
    df_cities_insert = df_cities.rename(columns={'city_ibge_code': 'cod_ibge', 'city': 'nome', 'estimated_population_2019': 'populacao', 'state': 'estado'})
    df_covid_insert = df_covid.rename(columns={'date': 'data','city_ibge_code': 'cod_ibge'})

    #inserts data on tables cidade and registro_covid
    conn, cursor = connect_db(params)
    execute_insert(cursor, df_cities_insert, "cidade")
    execute_insert(cursor, df_covid_insert, "registro_covid")

    conn.close()
    cursor.close()

if __name__ == "__main__":
    main()