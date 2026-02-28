import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib.colors as colors
import json
import requests
import csv

# --- CONFIGURACIÓ API METEOCAT ---
API_KEY = "5Rq09hMMoQ8JKQ87M9RxL5wM0dIW4HsU27G0WEjo" 
BASE_URL = "https://api.meteo.cat/xema/v1"
CODI_PLUJA = "1300"

OUTPUT_DIR = "dades_radar"
DAILY_DIR = "acumulats_diaris"

def get_stations_daily_data(date_obj):
    """Obté la pluja i coordenades de les estacions per a un dia concret"""
    headers = {"X-Api-Key": API_KEY}
    date_api_str = date_obj.strftime("%Y-%m-%dZ")
    
    print(f"📡 Descarregant dades i coordenades d'estacions per al {date_obj.strftime('%Y-%m-%d')}...")
    
    res_est = requests.get(f"{BASE_URL}/estacions/metadades", headers=headers)
    estacions_info = {}
    if res_est.status_code == 200:
        for e in res_est.json():
            estacions_info[e['codi']] = {
                'nom': e['nom'],
                'lat': e['coordenades']['latitud'],
                'lon': e['coordenades']['longitud']
            }

    url = f"{BASE_URL}/variables/estadistics/diaris/{CODI_PLUJA}?any={date_obj.year}&mes={date_obj.month:02d}"
    res = requests.get(url, headers=headers)
    
    dades_completes = []
    if res.status_code == 200:
        for estacio in res.json():
            codi = estacio.get('codiEstacio')
            if codi in estacions_info:
                info = estacions_info[codi]
                for val in estacio.get('valors', []):
                    if val['data'] == date_api_str:
                        dades_completes.append({
                            'codi': codi,
                            'nom': info['nom'],
                            'lat': info['lat'],
                            'lon': info['lon'],
                            'data': date_api_str.replace('Z',''),
                            'pluja': float(val['valor'])
                        })
    
    return dades_completes

def calculate_daily():
    # 1. Determinar el dia d'ahir
    ieri_obj = datetime.utcnow() - timedelta(days=1)
    ieri = ieri_obj.strftime("%Y%m%d")
    print(f"📅 Iniciant procés per al dia: {ieri}")

    if not os.path.exists(DAILY_DIR):
        os.makedirs(DAILY_DIR)

    # --- BLOC RADAR ---
    all_files_paths = []
    if os.path.exists(OUTPUT_DIR):
        for root, dirs, filenames in os.walk(OUTPUT_DIR):
            for filename in filenames:
                if filename.startswith(f"radar_{ieri}") and filename.endswith(".nc"):
                    all_files_paths.append(os.path.join(root, filename))
    
    all_files_paths.sort()

    if not all_files_paths:
        print(f"⚠️ No s'han trobat fitxers de radar per al dia {ieri}. Es saltarà l'acumulat de radar.")
    else:
        # Si hi ha fitxers, processem el radar
        total_precip = None
        used_files = []
        FACTOR_TEMPORAL = 0.1 

        for file_path in all_files_paths:
            try:
                with xr.open_dataset(file_path) as ds:
                    data = ds['precipitacio'].fillna(0).load()
                    if total_precip is None:
                        total_precip = data * FACTOR_TEMPORAL
                        lon, lat = ds['lon'].load(), ds['lat'].load()
                    else:
                        total_precip += data * FACTOR_TEMPORAL
                    used_files.append(os.path.basename(file_path))
            except Exception as e:
                print(f"⚠️ Error obrint {file_path}: {e}")

        if total_precip is not None:
            # 4. Crear NetCDF diari
            ds_daily = xr.Dataset(
                {"precipitacio_acumulada": (["lat", "lon"], total_precip.values)},
                coords={"lon": lon, "lat": lat},
                attrs={
                    "description": f"Acumulat diari {ieri}", 
                    "units": "mm", 
                    "date": ieri,
                    "files_count": len(used_files),
                    "resolution_min": 6
                }
            )
            nc_out_path = os.path.join(DAILY_DIR, f"acumulat_{ieri}.nc")
            ds_daily.to_netcdf(nc_out_path)
            print(f"✅ NetCDF diari guardat: {nc_out_path}")

            # 5. TXT fonts
            txt_out_path = os.path.join(DAILY_DIR, f"fonts_acumulat_{ieri}.txt")
            with open(txt_out_path, "w") as f_txt:
                f_txt.write(f"Resum de l'acumulat del dia {ieri}:\nTotal fitxers processats: {len(used_files)}\n\n")
                f_txt.write("\n".join(used_files))
            
            # 6. GENERAR PNG (Només si hi ha dades de radar)
            generate_daily_png(total_precip, lon, lat, ieri)

            # 7. BORRAR RADARS
            print(f"🗑️ Netejant fitxers temporals de radar...")
            for f in used_files:
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except:
                    pass

    # --- BLOC ESTACIONS (S'executa sempre) ---
    try:
        estacions_data = get_stations_daily_data(ieri_obj)
        if estacions_data:
            # 1. Guardar CSV
            csv_path = os.path.join(DAILY_DIR, f"estacions_{ieri}.csv")
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Codi', 'Nom', 'Data', 'Precipitacio_mm'])
                for d in estacions_data:
                    writer.writerow([d['codi'], d['nom'], d['data'], d['pluja']])
            print(f"📊 CSV d'estacions guardat: {csv_path}")

            # 2. Guardar GeoJSON
            geojson_path = os.path.join(DAILY_DIR, f"estacions_{ieri}.json")
            save_stations_geojson(estacions_data, geojson_path)
            print(f"📍 GeoJSON d'estacions guardat: {geojson_path}")
        else:
            print("⚠️ No s'han trobat dades d'estacions per a aquest dia.")
    except Exception as e:
        print(f"❌ Error processant dades d'estacions: {e}")

def generate_daily_png(data, lon, lat, date_str):
    fig = plt.figure(frameon=False)
    fig.set_size_inches(data.shape[1]/100, data.shape[0]/100)
    ax = plt.Axes(fig, [0., 0., 1., 1.])
    ax.set_axis_off()
    fig.add_axes(ax)
    norm = colors.LogNorm(vmin=0.1, vmax=200)
    cmap = plt.get_cmap('turbo').copy()
    cmap.set_under(alpha=0)
    ax.pcolormesh(lon.values, lat.values, data.values, cmap=cmap, norm=norm, shading='auto')
    png_out_path = os.path.join(DAILY_DIR, f"acumulat_{date_str}.png")
    fig.savefig(png_out_path, transparent=True, dpi=100)
    plt.close(fig)
    print(f"🎨 PNG guardat: {png_out_path}")

    bounds_data = {
        "lat_min": float(lat.min()), "lat_max": float(lat.max()),
        "lon_min": float(lon.min()), "lon_max": float(lon.max())
    }
    with open("bounds.json", "w") as f:
        json.dump(bounds_data, f)
    print(f"📍 Coordenades actualitzades a bounds.json")

def save_stations_geojson(dades, path):
    geojson = {"type": "FeatureCollection", "features": []}
    for d in dades:
        if d['pluja'] >= 0:
            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [d['lon'], d['lat']]},
                "properties": {
                    "codi": d['codi'], "nom": d['nom'],
                    "pluja": d['pluja'], "data": d['data']
                }
            }
            geojson["features"].append(feature)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)

if __name__ == "__main__":
    calculate_daily()





















