import os
import requests
import csv
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from datetime import datetime, timedelta
from collections import defaultdict
import json

# --- CONFIGURACIÓ ---
API_KEY = "5Rq09hMMoQ8JKQ87M9RxL5wM0dIW4HsU27G0WEjo" # Considera usar secrets de GitHub
BASE_URL = "https://api.meteo.cat/xema/v1"
CODI_PLUJA = "1300"
DAILY_DIR = "acumulats_diaris"
WEEKLY_DIR = "acumulats_setmanals"

def get_last_week_dates():
    # Avui és dilluns. Ahir era diumenge (fi), fa 7 dies era dilluns (inici)
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=7)
    end_date = today - timedelta(days=1)
    return start_date, end_date

def check_stations_rain(start_date, end_date):
    print(f"🔍 Validant dades amb estacions des de {start_date} fins a {end_date}...")
    
    # 1. Obtenir metadades estacions
    headers = {"X-Api-Key": API_KEY}
    res_est = requests.get(f"{BASE_URL}/estacions/metadades", headers=headers)
    dict_noms = {e['codi']: e['nom'] for e in res_est.json()} if res_est.status_code == 200 else {}

    # 2. Obtenir dades diàries de pluja per als mesos implicats
    # (Si la setmana canvia de mes, necessitem demanar ambdós)
    mesos_a_demanar = { (start_date.year, start_date.month), (end_date.year, end_date.month) }
    dades_estacions = []
    
    for any_q, mes_q in mesos_a_demanar:
        url = f"{BASE_URL}/variables/estadistics/diaris/{CODI_PLUJA}?any={any_q}&mes={mes_q:02d}"
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            dades_estacions.extend(res.json())

    # 3. Analitzar dia a dia
    validesa_dies = {} # { '20260216': {'valid': True, 'max_nom': '...', 'max_val': 1.2} }
    registres_csv = []

    current = start_date
    while current <= end_date:
        data_str_api = current.strftime("%Y-%m-%dZ")
        data_str_id = current.strftime("%Y%m%d")
        
        max_val = -1.0
        max_nom = "Cap"
        pluja_significativa = False
        
        for estacio in dades_estacions:
            codi = estacio.get('codiEstacio')
            nom = dict_noms.get(codi, codi)
            for val in estacio.get('valors', []):
                if val['data'] == data_str_api:
                    v = float(val['valor'])
                    registres_csv.append([codi, nom, data_str_api.replace('Z',''), v])
                    if v > max_val:
                        max_val = v
                        max_nom = nom
                    if v >= 1.0:
                        pluja_significativa = True
        
        validesa_dies[data_str_id] = {
            'valid': pluja_significativa,
            'max_nom': max_nom,
            'max_val': max_val
        }
        current += timedelta(days=1)

    return validesa_dies, registres_csv

def generate_weekly_accumulation(start_date, end_date, validesa):
    if not os.path.exists(WEEKLY_DIR): os.makedirs(WEEKLY_DIR)
    
    total_precip = None
    lon, lat = None, None
    dies_usats = []
    resum_txt = []

    resum_txt.append(f"RESUM SETMANAL: {start_date} a {end_date}")
    resum_txt.append("-" * 50)

    current = start_date
    while current <= end_date:
        dia_id = current.strftime("%Y%m%d")
        info = validesa.get(dia_id)
        
        if info['valid']:
            fitxer = os.path.join(DAILY_DIR, f"acumulat_{dia_id}.nc")
            if os.path.exists(fitxer):
                with xr.open_dataset(fitxer) as ds:
                    data = ds['precipitacio_acumulada'].load()
                    if total_precip is None:
                        total_precip = data
                        lon, lat = ds['lon'].load(), ds['lat'].load()
                    else:
                        total_precip += data
                dies_usats.append(dia_id)
                resum_txt.append(f"{dia_id}: PLUJA      -> Màxim: {info['max_nom']} ({info['max_val']} mm)")
            else:
                resum_txt.append(f"{dia_id}: ERROR      -> Fitxer .nc no trobat")
        else:
            resum_txt.append(f"{dia_id}: ANTICICLÓ  -> Màxim: {info['max_nom']} ({info['max_val']} mm) [DIA IGNORAT]")
        
        current += timedelta(days=1)

    if total_precip is None:
        print("⚠️ Setmana sense pluja vàlida. No es genera .nc")
        return resum_txt, None, None, None

    # Guardar resultats
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    nc_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.nc")
    
    ds_weekly = xr.Dataset(
        {"precipitacio_setmanal": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={"description": f"Acumulat setmana {week_str}", "units": "mm"}
    )
    ds_weekly.to_netcdf(nc_path)
    
    return resum_txt, total_precip, lon, lat

def save_outputs(start_date, end_date, resum, csv_data, data_array, lon, lat):
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    # 1. CSV Estacions
    csv_path = os.path.join(WEEKLY_DIR, f"estacions_{week_str}.csv")
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Codi', 'Nom', 'Data', 'Precipitacio_mm'])
        writer.writerows(csv_data)

    # 2. TXT Resum
    txt_path = os.path.join(WEEKLY_DIR, f"resum_{week_str}.txt")
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(resum))

    # 3. PNG Setmanal
    if data_array is not None:
        png_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.png")
        fig = plt.figure(frameon=False)
        fig.set_size_inches(data_array.shape[1]/100, data_array.shape[0]/100)
        ax = plt.Axes(fig, [0., 0., 1., 1.])
        ax.set_axis_off()
        fig.add_axes(ax)
        
        norm = colors.LogNorm(vmin=0.1, vmax=500) # Més vMax per a setmana
        cmap = plt.get_cmap('turbo').copy()
        cmap.set_under(alpha=0)
        
        ax.pcolormesh(lon.values, lat.values, data_array.values, cmap=cmap, norm=norm, shading='auto')
        fig.savefig(png_path, transparent=True, dpi=100)
        plt.close(fig)

if __name__ == "__main__":
    start, end = get_last_week_dates()
    validesa, registres_csv = check_stations_rain(start, end)
    resum, data, lon, lat = generate_weekly_accumulation(start, end, validesa)
    save_outputs(start, end, resum, registres_csv, data, lon, lat)
    print("✅ Procés setmanal finalitzat.")
