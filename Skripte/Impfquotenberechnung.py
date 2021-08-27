import pandas as pd
from datetime import date
date = date.today()

ImpfLandkreise = pd.read_csv("Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv", encoding='utf-8', low_memory=False)
GeoLandkreise = pd.read_csv("2020-06-30_Deutschland_Landkreise_GeoDemo.csv")
GeoLandkreise.drop(["Flaeche", "EW_maennlich", "EW_weiblich"], inplace=True, axis=1)
GeoLandkreise['Impfschutz_uv']=''
GeoLandkreise['Impfschutz_v']=''

for id in GeoLandkreise(['IdLandkreis']):
    i=0
    for j in ImpfLandkreise(['LandkreisId_Impfort']):
            if id=j:
                
                e
                
        
