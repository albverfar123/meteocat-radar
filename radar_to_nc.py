import requests
import math
import os
import numpy as np
import xarray as xr
import rasterio
import json
from datetime import datetime, timedelta
from io import BytesIO
from PIL import Image
from rasterio.transform import from_bounds

# --- CONFIGURACIÓ ---
ZOOM = 7
OUTPUT_DIR = "dades_radar"

TILES = [(64, 80), (65, 80), (64, 79), (65, 79)]

LLEGENDA_RADAR = {
    (128, 0, 255): 0.2,   (64, 0, 255): 0.8,   (0, 0, 255): 1.2,
    (0, 255, 255): 2.0,   (0, 255, 128): 3.0,   (0, 255, 0): 4.5,
    (63, 255, 0): 6.5,    (128, 255, 0): 9.0,   (198, 255, 0): 12.0,
    (255, 255, 0): 15.0,  (255, 171, 0): 15.1,  (255, 129, 0): 20.0,
    (255, 87, 0): 30.0,   (255, 45, 0): 40.0,   (255, 0, 0): 50.0,
    (255, 0, 63): 60.0,   (255, 0, 127): 70.0,  (255, 0, 191): 85.0,
    (255, 0, 255): 100.0, (255, 255, 255): 150.0
}

def tms_to_xyz(y, z):
    return (2 ** z - 1) - y

def tile_bounds_tms(x, y, z):
    y_xyz = tms_to_xyz(y, z)
    n = 2 ** z
    lon_left = x / n * 360.0 - 180.0
    lon_right = (x + 1) / n * 360.0 - 180.0
    lat_top = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y_xyz / n))))
    lat_bottom = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y_xyz + 1) / n))))
    return lon_left, lat_bottom, lon_right, lat_top

def build_url(ts, x, y):
    return f"https://static-m.meteo.cat/tiles/radar/{ts:%Y/%m/%d/%H/%M/%S}/000/000/{x:03d}/000/000/{y:03d}.png"

def find_latest_available_timestamp():
    now = datetime.utcnow()
    sample_x, sample_y = TILES[0]
    for i in range(60):
        check_ts = now - timedelta(minutes=i)
        for second in [7, 0]:
            test_ts = check_ts.replace(second=second, microsecond=0)
            url = build_url(test_ts, sample_x, sample_y)
            try:
                r = requests.head(url, timeout=3)
                if r.status_code == 200:
                    return test_ts
            except:
                continue
    return None

def process():
    # Crear carpeta de sortida si no existeix
    if not os.path.exists(OUTPUT_DIR): 
        os.makedirs(OUTPUT_DIR)

    target_ts = find_latest_available_timestamp()
    if not target_ts: 
        print("❌ No s'ha trobat cap timestamp disponible.")
        return

    tiles_data = {}
    for x, y in TILES:
        r = requests.get(build_url(target_ts, x, y), timeout=10)
        if r.status_code == 200:
            tiles_data[(x, y)] = np.array(Image.open(BytesIO(r.content)).convert("RGB"))
        else:
            print(f"⚠️ Error descarregant tile {x},{y}")
            return

    # Muntar el mosaic
    top = np.hstack([tiles_data[(64, 80)], tiles_data[(65, 80)]])
    bottom = np.hstack([tiles_data[(64, 79)], tiles_data[(65, 79)]])
    mosaic = np.vstack([top, bottom]).astype(np.uint8)

    height, width, _ = mosaic.shape
    
    # --- GENERACIÓ DE LES DADES DE PRECIPITACIÓ ---
    precip_data = np.full((height, width), np.nan, dtype=np.float32)
    r_chan, g_chan, b_chan = mosaic[:,:,0], mosaic[:,:,1], mosaic[:,:,2]

    for color, value in LLEGENDA_RADAR.items():
        mask = (r_chan == color[0]) & (g_chan == color[1]) & (b_chan == color[2])
        precip_data[mask] = value

    # Càlcul de coordenades
    bounds_list = [tile_bounds_tms(x, y, ZOOM) for x, y in TILES]
    transform = from_bounds(min(b[0] for b in bounds_list), min(b[1] for b in bounds_list), 
                            max(b[2] for b in bounds_list), max(b[3] for b in bounds_list), width, height)

    lons, _ = rasterio.transform.xy(transform, [0] * width, np.arange(width))
    _, lats = rasterio.transform.xy(transform, np.arange(height), [0] * height)

    # --- GENERACIÓ DEL NETCDF ---
    ds = xr.Dataset(
        {"precipitacio": (["lat", "lon"], precip_data)},
        coords={
            "lon": ("lon", np.array(lons), {"units": "degrees_east"}),
            "lat": ("lat", np.array(lats), {"units": "degrees_north"}),
        },
        attrs={"timestamp_utc": target_ts.strftime("%Y-%m-%d %H:%M:%S")}
    )
    
    nc_filename = f"radar_{target_ts:%Y%m%d_%H%M%S}.nc"
    ds.to_netcdf(os.path.join(OUTPUT_DIR, nc_filename))
    print(f"✅ Creat NC: {nc_filename}")

    # --- ACTUALITZACIÓ DE BOUNDS.JSON ---
    lat_min, lat_max = float(ds.lat.min()), float(ds.lat.max())
    lon_min, lon_max = float(ds.lon.min()), float(ds.lon.max())
    
    bounds_data = {
        "lat_min": lat_min,
        "lat_max": lat_max,
        "lon_min": lon_min,
        "lon_max": lon_max
    }
    
    with open("bounds.json", "w") as f:
        json.dump(bounds_data, f)

    print(f"📍 Coordenades actualitzades: {bounds_data}")

if __name__ == "__main__":
    process()


