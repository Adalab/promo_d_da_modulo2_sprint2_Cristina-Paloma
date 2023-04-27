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

df_ccaa_final['CCAA'] = df_ccaa_final['CCAA'].map(se.mapa_ccaa, na_action="ignore")


api.limpiar_energia(df_años)
api.limpiar_energia(df_ccaa_final)
api.cambiar_fecha(df_años, "datetime")
api.cambiar_fecha(df_ccaa_final, "datetime")

print(df_años)
print(df_ccaa_final)

df_años.to_csv("datos_años.csv")
df_ccaa_final.to_csv("datos_ccaa.csv")




carga = se.Cargar('energía', 'AlumnaAdalab')

carga.crear_bbdd()


carga.crear_insertar_tabla('''CREATE TABLE IF NOT EXISTS `energía`.`comunidades` (
                            `idcomunidades` INT NOT NULL AUTO_INCREMENT,
                            `comunidad` VARCHAR(45),
                            PRIMARY KEY (`idcomunidades`))
                            ENGINE = InnoDB;
                            ''')


carga.crear_insertar_tabla("""CREATE TABLE IF NOT EXISTS `energía`.`nacional_renovable_no_renovable`(
                            `idnacional` INT NOT NULL AUTO_INCREMENT,
                            `porcentaje` DECIMAL,
                            `tipo_energia` VARCHAR(45),
                            `valor` DECIMAL,
                            PRIMARY KEY (`idnacional`))
                            ENGINE = InnoDB;""")

carga.crear_insertar_tabla("""CREATE TABLE IF NOT EXISTS `energía`.`comunidades_renovable_no_renovable`(
                            `id` INT NOT NULL AUTO_INCREMENT,
                            `porcentaje` DECIMAL,
                            `tipo_energia` VARCHAR(45),
                            `valor` DECIMAL,
                            `idcomunidades` INT NOT NULL,
                            PRIMARY KEY (`id`),
                            CONSTRAINT `fk_comunidades_comunidades`
                                FOREIGN KEY (`idcomunidades`)
                                REFERENCES `energía`.`comunidades` (`idcomunidades`))
                            ENGINE = InnoDB;""")

carga.crear_insertar_tabla("""CREATE TABLE IF NOT EXISTS `energía`.`fechas`(
                                `idFechas` INT NOT NULL AUTO_INCREMENT,
                                `fecha` DATE,
                                `nacional_renovable_no_renovable_id` INT,
                                `comunidades_renovable_no_renovable_id` INT,
                                PRIMARY KEY (`idFechas`),
                                CONSTRAINT `fk_nacional_fechas`
                                    FOREIGN KEY(`nacional_renovable_no_renovable_id`)
                                    REFERENCES `energía`.`nacional_renovable_no_renovable`(`idnacional`),
                                CONSTRAINT `fk_comunidades_fechas`
                                    FOREIGN KEY (`comunidades_renovable_no_renovable_id`)
                                    REFERENCES `energía`.`comunidades_renovable_no_renovable` (`id`))
                                ENGINE = InnoDB;""")
