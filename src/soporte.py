import requests
import pandas as pd
import numpy as np
import mysql.connector
from geopy.geocoders import Nominatim

class Extraccion:
    
    ''' 
    Una clase para extraer datos de universidades de paises especificados de la API de hipolabs, y limpiarlos.
    
    Atributos:
    - lista de paises de los cuales queremos obtener los datos 
    
    Metodos
    llamar_API()
    - realiza dos tareas de limpieza:
        - reemplaza los guiones por guiones bajos en los nombres de las columnas
        - elimina la columna de domains
    
    limpieza_columnas(dataframe)
    - acepta una query de MySQL y segun ella, crea una tabla o inserta datos en una tabla en una base de datos en MySQL
    
    explode_columna(dataframe, columna)
    - separa los valores de la columna de un dataframe que estan en listas, creando una fila por elemento
    
    eliminar_dup(dataframe, columna)
    - elimina las filas con valores duplicados de la columna indicada del dataframe
    
    none_nan(dataframe, columna)
    - reemplaza cualquier valor de None por np.nan en la columna indicada del dataframe
    
    reemplazar_nulos(self, dataframe, columna, valor_nuevo)
     -reemplaza cualquier valor nulo por el valor nuevo indicado en la columna indicada
    
    reemplazar_valores(dataframe, columna, diccionario)
    reemplaza los valores de la columna indicada del dataframe segun el diccionario
    
    lat_long_merge(self, dataframe, user_agent)
    - devuelve el dataframe con columnas para el latitud y longitud de las provincias
    de los universidades, sacados con el API de geopy
    '''
    
    def __init__(self, lista_paises):
        
        '''
        Construye los atributos del objeto de extraccion de datos del API de universidades.
        
        Parametros
        lista_paises: lista
        - una lista de paises de los cuales queremos obtener los datos
        '''
        
        self.lista_paises = lista_paises
        
        
    def llamar_API(self):
        
        """
        Acepta una lista de países y devuelve un dataframe de los datos de universidades de los países indicados
        obtenidos de la API de hipolabs.
        
        Parametros:
        self.lista_paises: lista de paises
        
        Returns:
        - dataframe con los datos unidos de todos los paises indicados
        """
        
        dataframe = pd.DataFrame()
        
        for p in self.lista_paises:
            url = f"http://universities.hipolabs.com/search?country={p}"
            response = requests.get(url=url)
            status = response.status_code
            razon = response.reason
            if status == 200:
                
                print(f'La peticion al API para {p} se ha realizado con éxito.')
            else: 
                print(f'Respuesta {status}: {razon}')
            
            df_pais = pd.json_normalize(response.json())
            
            dataframe = pd.concat([dataframe, df_pais], axis=0)
        
        print(f'Sus datos ya están listos en un dataframe. Aquí tiene los primeros 3 filas:')
        
        display(dataframe.head(3))
        
        return dataframe
    
    
    def limpieza_columnas(self, dataframe):
    
        """
        Acepta un dataframe y realiza dos tareas de limpieza:
        - reemplaza los guiones por guiones bajos en los nombres de las columnas
        - elimina la columna de domains
        
        Parametros:
        - dataframe de datos extraidos del API de universidades de hipolabs
        
        Returns:
        - none
        """
        
              
        diccionario = {col : col.replace('-', '_') for col in dataframe.columns}

        dataframe.rename(columns=diccionario, inplace=True)
        
        dataframe.drop(columns='domains', axis=1, inplace=True)
        
        print("Su dataframe se ha limpiado. Aquí tiene la primera fila:")
        display(dataframe.head(1))
        
        
    def explode_columna(self, dataframe, columna):
        '''
        Acepta un dataframe y columna y separa los valores de la columna que estan en listas, creando una fila por elemento
        
        Parametros:
        - dataframe
        - string: el nombre de una columna del dataframe
        
        Returns:
        - un dataframe
        '''        
        return dataframe.explode(columna)
    
    
    def eliminar_dup(self, dataframe, columna):
    
        '''
        Acepta un dataframe y una columna y elimina las filas con valores duplicados de la columna indicada
        
        Parametros:
        - un dataframe
        - string: nombre de una columna del dataframe
        '''
            
        num_dup = dataframe[columna].duplicated().sum()
        dataframe.drop_duplicates(subset=columna, inplace=True)
        
        print(f'Se han eliminado {num_dup} filas del dataframe.')
        
    
    def none_nan(self, dataframe, columna):
    
        '''
        Acepta un dataframe y una columna y reemplaza cualquier valor de None por np.nan en 
        la columna indicada.
        
        Parametros:
        - dataframe
        - string: nombre de una columna del dataframe
        '''    
            
        num_none = dataframe[columna].isna().sum()
        
        dataframe[columna].fillna(value=np.nan, inplace=True)
        
        print(f'Se han reemplazado {num_none} valores None por np.nan en la columna {columna}.')
        
    
    def reemplazar_nulos(self, dataframe, columna, valor_nuevo):
        
        '''
        Acepta un dataframe y una columna y reemplaza cualquier valor nulo por el valor nuevo indicado en 
        la columna indicada.
        
        Parametros:
        - un dataframe
        - string: nombre de una columna del dataframe
        - string: valor nuevo que queremos en vez de nulos
        
        Returns:
        - none
        '''    
        
        num_none = dataframe[columna].isnull().sum()
        
        dataframe[columna].fillna(value=valor_nuevo, inplace=True)
        
        print(f'Se han reemplazado {num_none} valores nulos en la columna {columna} por {valor_nuevo}.')
        
        
    def reemplazar_valores(self, dataframe, columna, diccionario):
        
        '''Acepta un dataframe, una columna y un diccionario y reemplaza 
        los valores de la columna indicada segun el diccionario.
        
        Parametros:
        - un dataframe
        - string: nombre de una columna del dataframe
        - un diccionario donde los keys son los valores actuales y los values son los valores nuevos deseados
        
        Returns:
        - none
        '''
        
        try:
            for k, v in diccionario.items():
                dataframe[columna].replace(to_replace=k, value=v, inplace=True)
            
            print(f'Se han reemplazado los valores segun el mapa. Ahora los valores unicos de la columna {columna} son:')
            print(dataframe['state_province'].unique())
            
        except:
            print('No se ha podido realizar la operación.')
            
    
    def lat_long_merge(self, dataframe, user_agent):
    
        '''
        Acepta un dataframe de datos de universidades del API de hipolabs y el
        nombre de user agent para usar con el API de geopy. Devuelve el dataframe 
        con dos columnas nuevas indicando la latitud y la longitud del estado o 
        provincia de cada universidad sacados con el API de geopy.
        
        Parametros:
        - dataframe de datos de universidades del API de hipolabs
        - string: un nombre para usar como user agent con el API de geopy
        
        Returns:
        - el dataframe origina con una columna para la latitud y una columna para la longitud    
        '''
        
        lista_prov = dataframe['state_province'].unique().tolist()
        
        diccionario ={'state_province':[], 'latitude': [], 'longitude': []}
        
        for i in lista_prov:
            if i == 'Unknown' or i is np.nan:
                pass
            else:
                try:
                    geolocator = Nominatim(user_agent=user_agent)
                    location = geolocator.geocode(i)
                    diccionario['state_province'].append(i)
                    diccionario['latitude'].append(location[1][0]) 
                    diccionario['longitude'].append(location[1][1]) 
                    
                except:
                    print(f'No se puede conseguir la latitud y longitud de {i}')
        
        df_lat_long = pd.DataFrame(diccionario)
        
        return dataframe.merge(df_lat_long, on='state_province', how='left')
    
    

class Cargar:
    
    ''' 
    Una clase para cargar datos en una base de datos de MySQL.
    
    Atributos:
    nombre_bbdd: string
    - el nombre de la base de datos que creamos
    contraseña: string
    - la contraseña para conectar con MySQL
    
    Metodos
    crear_bbdd()
    - crea una base de datos en MySQL
    
    crear_insertar_tabla(query)
    - acepta una query de MySQL y segun ella, crea una tabla o inserta datos en una tabla en una base de datos en MySQL
    
    devolver_datos(query)
    - acepta una query de MySQL tipo SELECT y segun ella devuelve datos en forma de dataframe
    '''
    
    def __init__(self, nombre_bbdd, contraseña):
        
        ''' 
        Construye los atributos necesarios para el objeto de cargar datos en MySQL.
        
        Parametros:
        nombre_bbdd: string
        - el nombre de la base de datos que creamos
        contraseña: string
        - la contraseña para conectar con MySQL
        '''
        
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña
        
        
    def crear_bbdd(self):
    
        '''
        Crea una base de datos nombrado segun el nombre guardado como atributo de la clase.
        Si la conexion falla o si la base de datos ya existe, devuelve un mensaje de error.
        
        Parametros:
        - string del nombre que poner a la base de datos
        - string con la contraseña de MySQL
        
        Returns:
        - none
        '''
        
        conexion = mysql.connector.connect(host="localhost",
                                        user="root",
                                        password=self.contraseña)
                                        
        print("La conexión a MySQL se ha realizado con exito.")
        
        cursor = conexion.cursor()

        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            print(cursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            
            
    def crear_insertar_tabla(self, query):
    
        '''
        Acepta tres strings: el nombre de la base de datos, la contraseña de MySQL y una
        query para crear una tabla en la base de datos indicado o para insertar datos en una
        tabla de esa bbdd. Si la conexion falla o si la tabla ya existe, devuelve un mensaje de error.
        
        Parametros:
        - string: nombre de la base de datos
        - string: contraseña para MySQL
        - string: query de creacion de tabla o insercion de datos
        
        Returns:
        - none
        '''
        
        conexion = mysql.connector.connect(user='root', password=self.contraseña,
                                        host='127.0.0.1', database=self.nombre_bbdd)
        cursor = conexion.cursor()
        
        try: 
            cursor.execute(query)
            conexion.commit() 

        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
                
    
    def devolver_datos(self, query):
    
        ''' 
        Acepta el nombre de una base de datos, la contraseña de MySQL, y una query de SELECT de MySQL, y devuelve un
        dataframe con los resultados del query. Si no funcion devuelve un mensaje de error.
        
        Parametros:
        - string: nombre de una base de datos existente en MySQL
        - string: contraseña de MySQL
        - string: query tipo SELECT de MySQL
        '''
        
        conexion = mysql.connector.connect(user='root', password=self.contraseña,
                                        host='127.0.0.1', database=self.nombre_bbdd)
        
        try: 
            return pd.read_sql_query(query, conexion)
        
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)  
        
    