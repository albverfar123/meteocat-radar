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
    # Busquem fitxers que comencin amb radar_YYYYMMDD
    if not os.path.exists(OUTPUT_DIR):
        print(f"❌ La carpeta {OUTPUT_DIR} no existeix.")
        return

    files = [f for f in os.listdir(OUTPUT_DIR)
             if f.startswith(f"radar_{ieri}") and f.endswith(".nc")]
    files.sort()

    if not files:
        print(f"❌ No s'han trobat fitxers per al dia {ieri}")
        # Llistem què hi ha per fer debug si cal
        print(f"Contingut de {OUTPUT_DIR}: {os.listdir(OUTPUT_DIR)[:5]}...") 
        return

    # 3. Carregar i sumar les dades
    total_precip = None
    used_files = []
    
    # FACTOR DE CONVERSIÓ: 
    # Si cada imatge és cada 6 minuts, representen 6/60 hores = 0.1 hores.
    # Si fossin cada 12 minuts, seria 0.2.
    FACTOR_TEMPORAL = 0.1 

    for f in files:
        file_path = os.path.join(OUTPUT_DIR, f)
        try:
            with xr.open_dataset(file_path) as ds:
                # .load() és essencial per poder esborrar el fitxer després
                data = ds['precipitacio'].fillna(0).load()

                if total_precip is None:
                    total_precip = data * FACTOR_TEMPORAL
                    lon, lat = ds['lon'].load(), ds['lat'].load()
                else:
                    total_precip += data * FACTOR_TEMPORAL

                used_files.append(f)

        except Exception as e:
            print(f"⚠️ Error obrint {f}: {e}")

    if total_precip is None:
        print("❌ No s'ha pogut processar cap fitxer correctament.")
        return

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

    # 5. TXT fonts (Amb comptador de fitxers per control)
    txt_out_path = os.path.join(DAILY_DIR, f"fonts_acumulat_{ieri}.txt")
    with open(txt_out_path, "w") as f_txt:
        f_txt.write(f"Resum de l'acumulat del dia {ieri}:\n")
        f_txt.write(f"Total fitxers processats: {len(used_files)}\n")
        f_txt.write(f"Resolució temporal: 6 minuts (factor 0.1)\n")
        f_txt.write("-" * 40 + "\n")
        f_txt.write("\n".join(used_files))
    print(f"📄 Llista de fonts guardada a: {txt_out_path}")

    # 6. GENERAR PNG
    generate_daily_png(total_precip, lon, lat, ieri)

    # 7. BORRAR RADARS (Neteja de la carpeta de treball)
    print(f"🗑️ Iniciant neteja de dades temporals a {OUTPUT_DIR}...")
    deleted_count = 0
    for f in used_files:
        file_to_delete = os.path.join(OUTPUT_DIR, f)
        try:
            os.remove(file_to_delete)
            deleted_count += 1
        except Exception as e:
            print(f"  ⚠️ No s'ha pogut esborrar {f}: {e}")

    print(f"✨ Neteja completada. S'han eliminat {deleted_count} fitxers.")


def generate_daily_png(data, lon, lat, date_str):
    fig = plt.figure(frameon=False)
    # Mantenim l'aspecte segons la mida de la matriu de dades
    fig.set_size_inches(data.shape[1]/100, data.shape[0]/100)

    ax = plt.Axes(fig, [0., 0., 1., 1.])
    ax.set_axis_off()
    fig.add_axes(ax)

    # El rang de colors per a un dia sencer sol ser millor fins a 100-200mm
    norm = colors.LogNorm(vmin=0.1, vmax=200)
    cmap = plt.get_cmap('turbo').copy()
    cmap.set_under(alpha=0) # Transparent per a zones sense pluja significativa

    ax.pcolormesh(lon.values, lat.values, data.values,
                  cmap=cmap, norm=norm, shading='auto')

    png_out_path = os.path.join(DAILY_DIR, f"acumulat_{date_str}.png")
    fig.savefig(png_out_path, transparent=True, dpi=100)
    plt.close(fig)
    print(f"🎨 PNG logarítmic guardat: {png_out_path}")

    # Actualitzar bounds.json per si hi ha hagut canvis en la graella
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






