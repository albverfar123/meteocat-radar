import os
import requests
import csv
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import json
from datetime import datetime, timedelta

# --- CONFIGURACIÓ ---
API_KEY = "5Rq09hMMoQ8JKQ87M9RxL5wM0dIW4HsU27G0WEjo" 
BASE_URL = "https://api.meteo.cat/xema/v1"
CODI_PLUJA = "1300"
DAILY_DIR = "acumulats_diaris"
WEEKLY_DIR = "acumulats_setmanals"

def get_last_week_dates():
    # Per defecte: d'ahir (diumenge) cap enrere 7 dies (dilluns)
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=7)
    end_date = today - timedelta(days=1)
    return start_date, end_date

def check_stations_rain(start_date, end_date):
    print(f"🔍 Validant dades i calculant acumulats per estació des de {start_date} fins a {end_date}...")
    headers = {"X-Api-Key": API_KEY}
    
    # 1. Obtenir metadades COMPLETES (incloent coordenades)
    res_est = requests.get(f"{BASE_URL}/estacions/metadades", headers=headers)
    estacions_meta = res_est.json() if res_est.status_code == 200 else []
    
    # Creem un diccionari per acumular la setmana real i guardar coordenades
    # Estructura: { 'CODI': {'nom': '...', 'lat': 0.0, 'lon': 0.0, 'total': 0.0} }
    stats_estacions = {}
    for e in estacions_meta:
        stats_estacions[e['codi']] = {
            'nom': e['nom'],
            'lat': e['coordenades']['latitud'],
            'lon': e['coordenades']['longitud'],
            'total': 0.0
        }

    # 2. Obtenir dades diàries de pluja (Mesos implicats)
    mesos_a_demanar = { (start_date.year, start_date.month), (end_date.year, end_date.month) }
    dades_api = []
    for any_q, mes_q in mesos_a_demanar:
        url = f"{BASE_URL}/variables/estadistics/diaris/{CODI_PLUJA}?any={any_q}&mes={mes_q:02d}"
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            dades_api.extend(res.json())

    # 3. Analitzar dia a dia i sumar acumulats
    validesa_dies = {} 
    registres_csv = []

    current = start_date
    while current <= end_date:
        data_str_api = current.strftime("%Y-%m-%dZ")
        data_str_id = current.strftime("%Y%m%d")
        max_val, max_nom, hi_ha_pluja = -1.0, "Cap", False
        
        for estacio in dades_api:
            codi = estacio.get('codiEstacio')
            for val in estacio.get('valors', []):
                if val['data'] == data_str_api:
                    v = float(val['valor'])
                    # Registre per al CSV diari global
                    registres_csv.append([codi, stats_estacions.get(codi,{}).get('nom', codi), data_str_api.replace('Z',''), v])
                    
                    # Sumem a l'acumulat setmanal de l'estació
                    if codi in stats_estacions:
                        stats_estacions[codi]['total'] += v
                    
                    if v > max_val:
                        max_val = v
                        max_nom = stats_estacions.get(codi,{}).get('nom', codi)
                    if v >= 1.0: 
                        hi_ha_pluja = True
        
        validesa_dies[data_str_id] = {'valid': hi_ha_pluja, 'max_nom': max_nom, 'max_val': max_val}
        current += timedelta(days=1)

    return validesa_dies, registres_csv, stats_estacions

def generate_weekly_accumulation(start_date, end_date, validesa):
    if not os.path.exists(WEEKLY_DIR): os.makedirs(WEEKLY_DIR)
    
    resum_txt = [f"RESUM SETMANAL: {start_date} a {end_date}", "-" * 50]
    total_precip, lon, lat = None, None, None

    # 1. BUSCAR PLANTILLA PER A LA MALLA
    plantilla_path = next((os.path.join(DAILY_DIR, f) for f in os.listdir(DAILY_DIR) if f.endswith(".nc")), None)
    
    if plantilla_path:
        with xr.open_dataset(plantilla_path) as ds_ref:
            total_precip = xr.zeros_like(ds_ref['precipitacio_acumulada'])
            lon, lat = ds_ref['lon'].load(), ds_ref['lat'].load()
    else:
        return ["ERROR: No hi ha fitxers .nc per fer de plantilla."], None, None, None

    # 2. SUMAR DIES
    current = start_date
    while current <= end_date:
        dia_id = current.strftime("%Y%m%d")
        info = validesa.get(dia_id)
        path_nc = os.path.join(DAILY_DIR, f"acumulat_{dia_id}.nc")
        existeix = os.path.exists(path_nc)

        if info['valid']:
            if existeix:
                with xr.open_dataset(path_nc) as ds:
                    total_precip += ds['precipitacio_acumulada'].load()
                resum_txt.append(f"{dia_id}: PLUJA      -> Vàlid: {info['max_nom']} ({info['max_val']} mm)")
            else:
                resum_txt.append(f"{dia_id}: ERROR      -> Fitxer NC no trobat")
        else:
            avis = "" if existeix else " [AVÍS: Fitxer NC no trobat]"
            resum_txt.append(f"{dia_id}: ANTICICLÓ  -> Màxim: {info['max_nom']} ({info['max_val']} mm){avis}")
        
        current += timedelta(days=1)

    # 3. GUARDAR NETCDF SETMANAL
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    nc_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.nc")
    
    ds_weekly = xr.Dataset(
        {"precipitacio_setmanal": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={"description": f"Acumulat setmanal {week_str}", "units": "mm"}
    )
    ds_weekly.to_netcdf(nc_path)
    
    return resum_txt, total_precip, lon, lat

def save_outputs(start_date, end_date, resum, csv_data, stats_estacions, data_array, lon, lat):
    week_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    # 1. CSV Estacions (Dades diàries desglossades)
    with open(os.path.join(WEEKLY_DIR, f"estacions_{week_str}.csv"), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Codi', 'Nom', 'Data', 'Precipitacio_mm'])
        writer.writerows(csv_data)

    # 2. TXT Resum
    with open(os.path.join(WEEKLY_DIR, f"resum_{week_str}.txt"), 'w', encoding='utf-8') as f:
        f.write("\n".join(resum))

    # 3. GEOJSON Estacions (Acumulat setmanal per al visor)
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    for codi, d in stats_estacions.items():
        if d['total'] >= 0.1:  # Només incloem estacions amb pluja mínima per no saturar
            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [d['lon'], d['lat']]},
                "properties": {
                    "nom": d['nom'],
                    "codi": codi,
                    "pluja": round(d['total'], 1)
                }
            }
            geojson["features"].append(feature)
            
    with open(os.path.join(WEEKLY_DIR, f"estacions_{week_str}.json"), 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)

    # 4. PNG Setmanal i Fitxer de Límits (Bounds)
    if data_array is not None:
        png_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.png")
        bounds_path = os.path.join(WEEKLY_DIR, f"setmanal_{week_str}.json_bounds") # Fitxer auxiliar

        # Guardem els límits reals extrets de les dades .nc
        bounds = {
            "lat_min": float(lat.values.min()),
            "lat_max": float(lat.values.max()),
            "lon_min": float(lon.values.min()),
            "lon_max": float(lon.values.max())
        }
        with open(bounds_path, 'w') as f:
            json.dump(bounds, f)

        # Generar el PNG (important bbox_inches='tight' i pad_inches=0)
        fig = plt.figure(frameon=False)
        fig.set_size_inches(data_array.shape[1]/100, data_array.shape[0]/100)
        ax = plt.Axes(fig, [0., 0., 1., 1.])
        ax.set_axis_off()
        fig.add_axes(ax)
        
        if np.max(data_array) > 0.1:
            norm = colors.LogNorm(vmin=0.1, vmax=300)
            cmap = plt.get_cmap('turbo').copy()
            cmap.set_under(alpha=0)
            ax.pcolormesh(lon.values, lat.values, data_array.values, cmap=cmap, norm=norm, shading='auto')
        
        fig.savefig(png_path, transparent=True, dpi=100, bbox_inches='tight', pad_inches=0)
        plt.close(fig)

if __name__ == "__main__":
    start, end = get_last_week_dates()
    val_dies, reg_csv, stats_setmanals = check_stations_rain(start, end)
    res, data, ln, lt = generate_weekly_accumulation(start, end, val_dies)
    save_outputs(start, end, res, reg_csv, stats_setmanals, data, ln, lt)
    print(f"✅ Procés setmanal finalitzat. Generat GeoJSON per a {len(stats_setmanals)} estacions.")
