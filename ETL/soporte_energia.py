import pandas as pd
import requests
import mysql.connector
from datetime import datetime, timedelta

class Extraccion_Energia:
    def __init__(self):
        self = self

    def llamada_años(self, año1, año2):
        self.año1 = año1
        self.año2 = año2

        df_final = pd.DataFrame()
        df_renov = pd.DataFrame()
        df_no_renov = pd.DataFrame()
        for año in range(año1,año2):
            url = f'https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={año1}-01-01T00:00&end_date={año2}-12-31T23:59&time_trunc=year'
            response = requests.get(url = url)

            renovables = pd.DataFrame(response.json()['included'][0]['attributes']['values'])
            df_renov = pd.concat([df_renov, renovables],  axis=0, ignore_index = True)
            df_renov['type'] = 'renovable'
        
        
            no_renovables = pd.DataFrame(response.json()['included'][1]['attributes']['values'])
            df_no_renov = pd.concat([df_no_renov, no_renovables],  axis=0, ignore_index = True)
            df_no_renov['type'] = 'no renovable'

        df_final = pd.concat([df_renov, df_no_renov],  axis=0, ignore_index = True)
        return df_final
    
    def llamada_ccaa(self, año, id_ccaa):
        self.año = año
        self.id_ccaa = id_ccaa
        
        df_final = pd.DataFrame()
        renovables = pd.DataFrame()
        no_renovables = pd.DataFrame()

        url = f'https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={año}-01-01T00:00&end_date={año}-12-31T23:59&time_trunc=year&geo_limit=ccaa&geo_ids={id_ccaa}'
        response_ccaa = requests.get(url = url)
        
        renovables = pd.DataFrame(response_ccaa.json()['included'][0]['attributes']['values'])
        renovables['type'] = 'renovable'
        no_renovables = pd.DataFrame(response_ccaa.json()['included'][1]['attributes']['values'])
        no_renovables['type'] = 'no renovable'

        df_final = pd.concat([renovables, no_renovables],  axis=0, ignore_index = True)
        df_final['CCAA'] = id_ccaa

        return df_final
    
    def borrar_columnas(self, df, col):
        self.df = df
        self.col = col
        df.drop(columns = col, axis = 1, inplace = True)
        return df

    def limpiar_energia(self, df):
        self.df = df
        #esta parte de la función redondea las columnas numéricas
        for col in df.columns:
            if df[col].dtype == float:
                df[col] = round(df[col],2)
            else:
                pass
        return df
    
    def cambiar_fecha(self, df, col):
        self.df = df
        self.col = col

        #esta función modifica la fecha al formato solicitado y la deja con el tipo datetime
        df[col] = ((df[col].apply(pd.to_datetime)).dt.strftime("%Y-%m-%d")).apply(pd.to_datetime)
        return df
    

cod_comunidades2 = {'Andalucía': 4,
                    'Aragón': 5,
                    'Cantabria': 6,
                    'Castilla - La Mancha': 7,
                    'Castilla y León': 8,
                    'Cataluña': 9,
                    'País Vasco': 10,
                    'Principado de Asturias': 11,
                    'Comunidad de Madrid': 13,
                    'Comunidad Foral de Navarra': 14,
                    'Comunitat Valenciana': 15,
                    'Extremadura': 16,
                    'Galicia': 17,
                    'Región de Murcia': 21,
                    'La Rioja': 20}

mapa_ccaa = {v:k for k, v in cod_comunidades2.items()}
