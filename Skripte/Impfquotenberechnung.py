import argparse
from datetime import date
import numpy as np
import pandas as pd
date = date.today()

def main(source_csv: str, geo_landkreise_csv: str, target_csv: str):
    aggregated_impfquoten = aggregate_impfquoten(source_csv)
    unstacked_impfquoten = unstack_impfquoten(aggregated_impfquoten)
    geo_landkreise = load_geo_daten(geo_landkreise_csv)
    merged = merge_impfquoten_with_geolandkreise(unstacked_impfquoten, geo_landkreise)
    new_names = {
            'IdLandkreis': 'LandkreisID',
            'Gemeindename': 'Landkreis',
            'EW_insgesamt': 'Einwohner_gesamt',
            '1': 'Impfschutz_uvoll',
            '2': 'Impfschutz_voll',
            '3': 'Impfschutz_aufgefrischt'}
    merged.rename(columns=new_names, inplace=True)
    merged['Quote_uvoll'] = merged.Impfschutz_uvoll / merged.Einwohner_gesamt
    merged['Quote_voll'] = merged.Impfschutz_voll / merged.Einwohner_gesamt
    merged['Quote_aufgefrischt'] = merged.Impfschutz_aufgefrischt / merged.Einwohner_gesamt
    column_order = [
            "LandkreisID",
            "Landkreis",
            "Einwohner_gesamt",
            "Impfschutz_uvoll",
            "Impfschutz_voll",
            "Impfschutz_aufgefrischt",
            "Quote_uvoll",
            "Quote_voll",
            "Quote_aufgefrischt"]
    merged = merged.reindex(columns=column_order)
    merged.to_csv(target_csv, index=False)

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

    summed = impf_landkreise.groupby(['LandkreisId_Impfort','Impfschutz'], as_index=False).sum()

    return summed

def unstack_impfquoten(impfquoten):
    unstacked = impfquoten.set_index(['LandkreisId_Impfort', 'Impfschutz']).unstack(level = -1)
    unstacked = unstacked.fillna(0)
    unstacked.columns = unstacked.columns.droplevel(0).rename('')
    return unstacked.reset_index()

def merge_impfquoten_with_geolandkreise(impfquoten, geo_landkreise):
    merged = pd.merge(
            impfquoten,
            geo_landkreise,
            how='left',
            left_on='LandkreisId_Impfort',
            right_on='IdLandkreis')
    merged.drop(['LandkreisId_Impfort'], inplace=True, axis=1)
    merged.reset_index(drop=True, inplace=True)
    return merged

def load_geo_daten(geo_landkreise_path):
    geo_landkreise = pd.read_csv(geo_landkreise_path)
    geo_landkreise.drop(["Flaeche", "EW_maennlich", "EW_weiblich"], inplace=True, axis=1)
    geo_landkreise['IdLandkreis'] = geo_landkreise['IdLandkreis'].apply(lambda x: str(x) if len(str(x)) > 4 else '0{}'.format(x))
    return geo_landkreise


if __name__ == "__main__":
    description='Aggregate and write impfdaten by landkreis and impfschutz'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
            '--source',
            default="https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/master/Aktuell_Deutschland_Landkreise_COVID-19-Impfungen.csv",
            help='The source CSV of landkreis aggregated Impfdata')
    parser.add_argument(
            '--geo_landkreise',
            default="./Skripte/2020-06-30_Deutschland_Landkreise_GeoDemo.csv",
            help='The source CSV of geo landkreis data')
    parser.add_argument(
            '--dest',
            default='./Aktuell_Deutschland_Quoten_COVID-19-Impfungen.csv',
            help='The destination CSV for the summed Impfquoten')

    args=parser.parse_args()
    main(args.source, args.geo_landkreise, args.dest)

                
        
