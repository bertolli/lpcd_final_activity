import pandas as pd
import psycopg2
import psycopg2.extras as extras


## BD
#Set your local variables here!
params = dict(
    database = "grupo_e",
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
#####

##Carega DF
df_completo = pd.read_csv("covid19_casos_brasil.csv")

#colunas cidade
cols_cidade = ['city_ibge_code', 'city', 'estimated_population_2019', 'state']

#cria um novo DF somente com as colunas desejadas
def select_columns(data_frame, column_names):
    new_frame = data_frame.loc[:, column_names]
    return new_frame


# Exemplo de cidade para filtrar
cidades = ['Sorocaba', 'Ponta Grossa', 'Curitiba', 'Manaus', 'São Paulo']

#Cria df apenas com as cidades no array
frames = list()
for c in cidades:
    frames.append(df_completo[df_completo['city'] == c])
    df_cidades = pd.concat(frames)

#apenas colunas desejadas
df_cidades = select_columns(df_cidades,cols_cidade)

#remove duplicados
df_cidades = df_cidades.drop_duplicates(subset=['city_ibge_code'])

#rename das colunas de acordo com o db
df_cidades_insert = df_cidades.rename(columns={'city_ibge_code': 'cod_ibge', 'city': 'nome', 'estimated_population_2019': 'populacao', 'state': 'estado'})


# Funcao insert atraves de um df
def execute_values(conn, datafrm, table):
    # Cria tupla
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # Cria colunas
    cols = ','.join(list(datafrm.columns))

    # Cria sql
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        print("Insert db successfully..")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close


execute_values(conn,df_cidades_insert, "cidade")

#Closing the connection
conn.close()
cursor.close()
print("Closing connection")