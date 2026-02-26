import os
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

OUTPUT_DIR = "dades_radar"
DAILY_DIR = "acumulats_diaris"

def calculate_daily():
    # 1. Determinar el dia d'ahir
    ieri = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"📅 Generant acumulat per al dia: {ieri}")

    for d in [DAILY_DIR]:
        if not os.path.exists(d): os.makedirs(d)

    # 2. Filtrar fitxers d'ahir
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(f"radar_{ieri}") and f.endswith(".nc")]
    files.sort()

    if not files:
        print(f"❌ No s'han trobat fitxers per al dia {ieri}")
        return

    # 3. Carregar i sumar les dades
    total_precip = None
    
    for f in files:
        ds = xr.open_dataset(os.path.join(OUTPUT_DIR, f))
        data = ds['precipitacio'].fillna(0)
        
        if total_precip is None:
            total_precip = data * 0.2
            lon, lat = ds['lon'], ds['lat']
        else:
            total_precip += data * 0.2
        ds.close()

    # 4. Crear el fitxer NetCDF
    ds_daily = xr.Dataset(
        {"precipitacio_acumulada": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={"description": f"Acumulat diari {ieri}", "units": "mm", "date": ieri}
    )
    
    nc_out_path = os.path.join(DAILY_DIR, f"acumulat_{ieri}.nc")
    ds_daily.to_netcdf(nc_out_path)
    print(f"✅ NetCDF diari guardat: {nc_out_path}")

    # 5. GENERACIÓ DEL PNG DE L'ACUMULAT
    generate_daily_png(total_precip, lon, lat, ieri)

def generate_daily_png(data, lon, lat, date_str):
    # Creem la figura sense eixos ni marges per tenir només la "capa" de pluja
    fig = plt.figure(frameon=False)
    fig.set_size_inches(width=data.shape[1]/100, height=data.shape[0]/100)
    ax = plt.Axes(fig, [0., 0., 1., 1.])
    ax.set_axis_off()
    fig.add_axes(ax)

    # Definim l'escala de colors (podes canviar 'viridis' per 'YlGnBu' o 'turbo')
    # vmin=0.1 fa que els valors de 0 siguin transparents si usem set_bad
    cmap = plt.get_cmap('turbo').copy()
    cmap.set_under(alpha=0) 

    # Pintem les dades. Fem servir vmin=0.1 per no pintar zones on no ha plogut
    ax.pcolormesh(lon, lat, data, cmap=cmap, vmin=0.1, vmax=40, shading='auto')

    png_out_path = os.path.join(DAILY_DIR, f"acumulat_{date_str}.png")
    fig.savefig(png_out_path, transparent=True, dpi=100)
    plt.close(fig)
    print(f"🎨 PNG diari guardat: {png_out_path}")

if __name__ == "__main__":
    calculate_daily()

