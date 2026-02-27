import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import matplotlib.colors as colors
import json

OUTPUT_DIR = "dades_radar"
DAILY_DIR = "acumulats_diaris"

def calculate_daily():
    # 1. Determinar el dia d'ahir
    ieri = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"📅 Generant acumulat per al dia: {ieri}")

    if not os.path.exists(DAILY_DIR): 
        os.makedirs(DAILY_DIR)

    # 2. Filtrar fitxers d'ahir
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"radar_{ieri}") and f.endswith(".nc")]
    files.sort()

    if not files:
        print(f"❌ No s'han trobat fitxers per al dia {ieri}")
        return

    # 3. Carregar i sumar les dades
    total_precip = None
    used_files = [] # Llista per guardar els noms dels fitxers utilitzats
    
    for f in files:
        try:
            ds = xr.open_dataset(os.path.join(OUTPUT_DIR, f))
            data = ds['precipitacio'].fillna(0)
            
            if total_precip is None:
                total_precip = data * 0.2
                lon, lat = ds['lon'], ds['lat']
            else:
                total_precip += data * 0.2
            
            used_files.append(f) # Afegim el nom a la llista si s'ha obert correctament
            ds.close()
        except Exception as e:
            print(f"⚠️ Error obrint {f}: {e}")

    # 4. Crear el fitxer NetCDF
    ds_daily = xr.Dataset(
        {"precipitacio_acumulada": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={"description": f"Acumulat diari {ieri}", "units": "mm", "date": ieri}
    )
    
    nc_out_path = os.path.join(DAILY_DIR, f"acumulat_{ieri}.nc")
    ds_daily.to_netcdf(nc_out_path)
    print(f"✅ NetCDF diari guardat: {nc_out_path}")

    # 5. GENERAR EL FITXER TXT AMB ELS NOMS DELS ARXIUS UTILITZATS
    txt_out_path = os.path.join(DAILY_DIR, f"fonts_acumulat_{ieri}.txt")
    with open(txt_out_path, "w") as f_txt:
        f_txt.write(f"Fitxers utilitzats per a l'acumulat del dia {ieri}:\n")
        f_txt.write("\n".join(used_files))
    print(f"📄 Llista de fonts guardada a: {txt_out_path}")

    # 6. GENERACIÓ DEL PNG DE L'ACUMULAT
    generate_daily_png(total_precip, lon, lat, ieri)

def generate_daily_png(data, lon, lat, date_str):
    fig = plt.figure(frameon=False)
    # Calculem la mida per mantenir aspecte original (pels 40mb que comentaves)
    fig.set_size_inches(data.shape[1]/100, data.shape[0]/100)
    
    ax = plt.Axes(fig, [0., 0., 1., 1.])
    ax.set_axis_off()
    fig.add_axes(ax)

    # Definim la norma logarítmica de 0.1 a 200 mm
    norm = colors.LogNorm(vmin=0.1, vmax=200)
    cmap = plt.get_cmap('turbo').copy()
    cmap.set_under(alpha=0) # Transparent per sota de 0.1mm

    # Dibuixem
    ax.pcolormesh(lon.values, lat.values, data.values, cmap=cmap, norm=norm, shading='auto')

    png_out_path = os.path.join(DAILY_DIR, f"acumulat_{date_str}.png")
    fig.savefig(png_out_path, transparent=True, dpi=100)
    plt.close(fig)
    print(f"🎨 PNG logarítmic guardat: {png_out_path}")
    
    # GUARDAR EL BOUNDS.JSON
    bounds_data = {
        "lat_min": float(lat.min()),
        "lat_max": float(lat.max()),
        "lon_min": float(lon.min()),
        "lon_max": float(lon.max())
    }
    with open("bounds.json", "w") as f:
        json.dump(bounds_data, f)
    print(f"📍 Coordenades guardades a bounds.json")

if __name__ == "__main__":
    calculate_daily()





