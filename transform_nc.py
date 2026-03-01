import os
import glob
import xarray as xr
import numpy as np

# Configuració
INPUT_FOLDER = 'dades_radar'

# Diccionari de transformació {valor_actual: valor_nou}
TRANSFORM_MAP = {
    0.2: 0.1,
    0.8: 0.4,
    1.2: 0.8,
    2.0: 1.0,
    3.0: 1.5,
    4.5: 2.0,
    6.5: 2.5,
    9.0: 3.0,
    12.0: 4.0,
    15.0: 5.0,
   15.1: 7.0,
    20.0: 10.0,
    30.0: 15.0,
    40.0: 20.0,
    50.0: 30.0,
    60.0: 40.0,
    70.0: 55.0,
    85.0: 70.0,
    100.0: 90.0,
    150.0: 120    
}

def transform_nc_files():
    arxius = glob.glob(os.path.join(INPUT_FOLDER, "*.nc"))
    
    if not arxius:
        print(f"No s'han trobat arxius .nc a {INPUT_FOLDER}")
        return

    for path in arxius:
        print(f"🔄 Transformant valors a: {os.path.basename(path)}...")
        
        try:
            # Obrim el dataset
            with xr.open_dataset(path) as ds:
                # Identifiquem la variable principal (la que no és lat, lon o time)
                var_name = [v for v in ds.data_vars if v not in ['lat', 'lon', 'time']][0]
                
                # Creem una còpia de les dades per modificar-les
                data_values = ds[var_name].values.copy()
                
                # Apliquem el mapeig de valors
                for old_val, new_val in TRANSFORM_MAP.items():
                    # Fem servir isclose per evitar problemes de precisió de coma flotant
                    mask = np.isclose(data_values, old_val, atol=0.001)
                    data_values[mask] = new_val
                
                # Assignem les noves dades a la variable del dataset
                ds[var_name].values = data_values
                
                # Guardem el fitxer temporalment i després substituïm l'original
                temp_path = path + ".tmp"
                ds.to_netcdf(temp_path)
            
            os.replace(temp_path, path)
            print(f"✅ Fitxer actualitzat amb èxit.")
            
        except Exception as e:
            print(f"❌ Error processant {path}: {e}")

if __name__ == "__main__":
    transform_nc_files()
