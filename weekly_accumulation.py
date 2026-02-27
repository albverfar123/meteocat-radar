import os
import requests
import csv
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from datetime import datetime, timedelta

# --- CONFIGURACIÓ ---
API_KEY = "5Rq09hMMoQ8JKQ87M9RxL5wM0dIW4HsU27G0WEjo" 
BASE_URL = "https://api.meteo.cat/xema/v1"
CODI_PLUJA = "1300"
DAILY_DIR = "acumulats_diaris"
WEEKLY_DIR = "acumulats_setmanals"

def get_last_week_dates():
    # Avui és dilluns (teòricament). Ahir diumenge, fa 7 dies dilluns.
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=7)
    end_date = today - timedelta(days=1)
    return start_date, end_date

def check_stations_rain(start_date, end_date):
    print(f"🔍 Validant dades amb estacions des de {start_date} fins a {end_date}...")
    headers = {"X-Api-Key": API_KEY}
    
    # 1. Metadades
    res_est = requests.get(f"{BASE_URL}/estacions/metadades", headers=headers)
    dict_noms = {e['codi']: e['nom'] for e in res_est.json()} if res_est.status_code == 200 else {}

    # 2. Dades de pluja (Mesos implicats)
    mesos_a_demanar = { (start_date.year, start_date.month), (end_date.year, end_date.month) }
    dades_estacions = []
    for any_q, mes_q in mesos_a_demanar:
        url = f"{BASE_URL}/variables/estadistics/diaris/{CODI_PLUJA}?any={any_q}&mes={mes_q:02d}"
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            dades_estacions.extend(res.json())

    # 3. Analitzar dia a dia
    validesa_dies = {} 
    registres_csv = []

    current = start_date
    while current <= end_date:
        data_str_api = current.strftime("%Y-%m-%dZ")
        data_str_id = current.strftime("%Y%m%d")
        max_val, max_nom, hi_ha_pluja = -1.0, "Cap", False
        
        for estacio in dades_estacions:
            codi = estacio.get('codiEstacio')
            nom = dict_noms.get(codi, codi)
            for val in estacio.get('valors', []):
                if val['data'] == data_str_api:
                    v = float(val['valor'])
                    registres_csv.append([codi, nom, data_str_api.replace('Z',''), v])
                    if v > max_val: max_val, max_nom = v, nom
                    if v >= 1.0: hi_ha_pluja = True
        
        validesa_dies[data_str_id] = {'valid': hi_ha_pluja, 'max_nom': max_nom, 'max_val': max_val}
        current += timedelta(days=1)

    return validesa_dies, registres_csv

def generate_weekly_accumulation(start_date, end_date, validesa):
    if not os.path.exists(WEEKLY_DIR): os.makedirs(WEEKLY_DIR)
    
    resum_txt = [f"RESUM SETMANAL: {start_date} a {end_date}", "-" * 50]
    total_precip, lon, lat = None, None, None

    # 1. BUSCAR UNA PLANTILLA (qualsevol .nc existent) PER REPLICAR LA MALLA
    plantilla_path = next((os.path.join(DAILY_DIR, f) for f in os.listdir(DAILY_DIR) if f.endswith(".nc")), None)
    
    if plantilla_path:
        with xr.open_dataset(plantilla_path) as ds_ref:
            total_precip = xr.zeros_like(ds_ref['precipitacio_acumulada'])
            lon, lat = ds_ref['lon'].load(), ds_ref['lat'].load()
    else:
        return ["ERROR: No s'ha trobat cap fitxer .nc diari a la carpeta per usar de plantilla."], None, None, None

    # 2. ITERAR DIES I SUMAR
    current = start_date
    while current <= end_date:
        dia_id = current.strftime("%Y%m%d")
        info = validesa.get(dia_id)
        path_nc = os.path.join(DAILY_DIR, f"acumulat_{dia_id}.nc")
        existeix_fitxer = os.path.exists(path_nc)

        if info['valid']:
            if existeix_fitxer:
                with xr.open_dataset(path_nc) as ds:
                    total_precip += ds['precipitacio_acumulada'].load()
                resum_txt.append(f"{dia_id}: PLUJA      -> Vàlid: {info['max_nom']} ({info['max_val']} mm)")
            else:
                resum_txt.append(f"{dia_id}: ERROR      -> Dia de pluja però FITXER NC NO TROBAT")
        else:
            avis_fitxer = "" if existeix_fitxer else " [AVÍS: Fitxer NC no trobat]"
            resum_txt.append(f"{dia_id}: ANTICICLÓ  -> Màxim: {info['max_nom']} ({info['max_val']} mm){avis_fitxer}")
        
        current += timedelta(days=1)

    # 3. GUARDAR EL NETCDF SETMANAL (Sempre es genera)
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    nc_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.nc")
    
    ds_weekly = xr.Dataset(
        {"precipitacio_setmanal": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={"description": f"Acumulat setmanal {week_str}", "units": "mm"}
    )
    ds_weekly.to_netcdf(nc_path)
    
    return resum_txt, total_precip, lon, lat

def save_outputs(start_date, end_date, resum, csv_data, data_array, lon, lat):
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    # 1. Guardar CSV
    with open(os.path.join(WEEKLY_DIR, f"estacions_{week_str}.csv"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Codi', 'Nom', 'Data', 'Precipitacio_mm'])
        writer.writerows(csv_data)

    # 2. Guardar TXT
    with open(os.path.join(WEEKLY_DIR, f"resum_{week_str}.txt"), 'w', encoding='utf-8') as f:
        f.write("\n".join(resum))

    # 3. Guardar PNG (Gestió de zeros per evitar error LogNorm)
    if data_array is not None:
        png_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.png")
        fig = plt.figure(frameon=False)
        fig.set_size_inches(data_array.shape[1]/100, data_array.shape[0]/100)
        ax = plt.Axes(fig, [0., 0., 1., 1.])
        ax.set_axis_off()
        fig.add_axes(ax)
        
        # Si no hi ha cap valor > 0, el LogNorm peta. Pintem un mapa transparent.
        if np.max(data_array) > 0.1:
            norm = colors.LogNorm(vmin=0.1, vmax=300)
            cmap = plt.get_cmap('turbo').copy()
            cmap.set_under(alpha=0)
            ax.pcolormesh(lon.values, lat.values, data_array.values, cmap=cmap, norm=norm, shading='auto')
        
        fig.savefig(png_path, transparent=True, dpi=100)
        plt.close(fig)

if __name__ == "__main__":
    start, end = get_last_week_dates()
    val_dies, reg_csv = check_stations_rain(start, end)
    res, data, ln, lt = generate_weekly_accumulation(start, end, val_dies)
    save_outputs(start, end, res, reg_csv, data, ln, lt)
    print("✅ Procés setmanal finalitzat.")
