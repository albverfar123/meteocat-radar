import os
import xarray as xr
import numpy as np
from datetime import datetime, timedelta

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

    print("📄 Arxius utilitzats:")
    for f in files:
        print(f"  - {f}")

    # 3. Carregar i sumar les dades
    total_precip = None
    
    for f in files:
        ds = xr.open_dataset(os.path.join(OUTPUT_DIR, f))
        # Convertim NaNs a 0 per poder sumar, però guardem on hi ha dades per la màscara final
        data = ds['precipitacio'].fillna(0)
        
        if total_precip is None:
            # Inicialitzem la matriu amb el primer fitxer (multiplicat per 0.2 hores)
            total_precip = data * 0.2
            lon, lat = ds['lon'], ds['lat']
        else:
            total_precip += data * 0.2
        
        ds.close()

    # 4. Crear el fitxer d'acumulat
    # Si un píxel ha estat 0 tot el dia, potser volem que torni a ser NaN 
    # (opcional, aquí el deixem com a valor numèric)
    
    ds_daily = xr.Dataset(
        {"precipitacio_acumulada": (["lat", "lon"], total_precip.values)},
        coords={"lon": lon, "lat": lat},
        attrs={
            "description": f"Acumulat diari de precipitació {ieri}",
            "units": "mm",
            "files_combined": len(files),
            "date": ieri
        }
    )
    
    ds_daily.precipitacio_acumulada.attrs['units'] = 'mm'
    
    out_path = os.path.join(DAILY_DIR, f"acumulat_{ieri}.nc")
    ds_daily.to_netcdf(out_path)
    print(f"✅ Acumulat diari guardat a: {out_path} (Total: {len(files)} arxius)")

if __name__ == "__main__":
    calculate_daily()