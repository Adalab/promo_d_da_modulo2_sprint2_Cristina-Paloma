import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector
import soporte_energia as se

# iniciamos la clase que en nuestro caso no toma atributos
api=se.Extraccion_Energia()

año1 = int(input("¿Desde qué año quieres los datos?"))
año2 = int(input("¿Hasta qué año quieres los datos?"))
           
df_años = api.llamada_años(año1, año2)


df_ccaa_final = pd.DataFrame()
for año in range(año1,año2+1):
    for k, v in se.cod_comunidades2.items(): 
        df = api.llamada_ccaa(año, v)
        df_ccaa_final = pd.concat([df_ccaa_final, df],  axis=0, ignore_index = True)


api.limpiar_energia(df_años)
api.limpiar_energia(df_ccaa_final)
api.cambiar_fecha(df_años, "datetime")
api.cambiar_fecha(df_ccaa_final, "datetime")

print(df_años)
print(df_ccaa_final)

df_años.to_csv("files/datos_años.csv")
df_ccaa_final.to_csv("files/datos_ccaa.csv")