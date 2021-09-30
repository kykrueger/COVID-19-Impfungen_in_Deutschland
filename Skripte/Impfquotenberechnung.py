import argparse
from datetime import date
import numpy as np
import pandas as pd
date = date.today()

def main(source_csv: str, target_csv: str):
    aggregated_impfquoten = aggregate_impfquoten(source_csv)
    aggregated_impfquoten.to_csv(target_csv)
    geo_landkreise = load_geo_daten()
    merged = pd.merge(
            aggregated_impfquoten,
            geo_landkreise,
            how='left',
            left_on='LandkreisId_Impfort',
            right_on='IdLandkreis')
    merged.drop(['LandkreisId_Impfort'], inplace=True, axis=1)
    merged.reset_index(drop=True, inplace=True)
    merged.to_csv(target_csv)

def aggregate_impfquoten(impfquoten_path: str):
    impfquoten_schema = {
            'Impfdatum': str,
            'LandkreisId_Impfort': str,
            'Altersgruppe': str,
            'Impfschutz': str,
            'Anzahl': np.int32 }
    
    impf_landkreise = pd.read_csv(
            impfquoten_path,
            encoding='utf-8',
            parse_dates=[1],
            dtype=impfquoten_schema)

    return impf_landkreise.groupby(['LandkreisId_Impfort','Impfschutz'], as_index=False).sum()

def load_geo_daten():
    geo_landkreise = pd.read_csv("./Skripte/2020-06-30_Deutschland_Landkreise_GeoDemo.csv")
    geo_landkreise.drop(["Flaeche", "EW_maennlich", "EW_weiblich"], inplace=True, axis=1)
    geo_landkreise['IdLandkreis'] = geo_landkreise['IdLandkreis'].apply(lambda x: str(x) if len(str(x)) > 4 else '0{}'.format(x))
    return geo_landkreise


if __name__ == "__main__":
    description='Aggregate and write impfdaten by landkreis and impfschutz'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
            '--source',
            default="./Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv",
            help='The source CSV of landkreis aggregated Impfdata')
    parser.add_argument(
            '--dest',
            default='./out.csv',
            help='The destination CSV for the summed Impfquoten')

    args=parser.parse_args()
    main(args.source, args.dest)

                
        
