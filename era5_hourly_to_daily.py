import zipfile
import os
import xarray as xr
import tempfile
import time

# Step 1: Define the input and output directories
# These directories should be updated based on the environment
input_dir = '/Users/Download/ERA5/input'
output_dir = '/Users/Download/ERA5/output'

# Step 2: Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Step 3: Initialize a timer to calculate runtime
start_time = time.time()

# Step 4: Loop through years and months
# In this exaple I choose 2000 to 2010
for year in range(2000, 2011):
    for month in range(1, 13):
        print(f"Processing year {year}, month {month}")

        # Step 5: Generate file paths for upper air and surface files
        upper_air_file = f'upper_air_{year:04d}_{month:02d}.nc'
        surface_file = f'surface_{year:04d}_{month:02d}.nc'
        upper_air_path = os.path.join(input_dir, upper_air_file)
        surface_path = os.path.join(input_dir, surface_file)

        # Step 6: Check if both files exist
        if not os.path.exists(upper_air_path) or not os.path.exists(surface_path):
            print(f"Skipping missing files: {upper_air_file}, {surface_file}")
            continue

        try:
            # Step 7: Read upper air data
            ds_upper_air = xr.open_dataset(upper_air_path, engine='h5netcdf').compute()

            # Step 8: Extract surface data from zip file
            with zipfile.ZipFile(surface_path, 'r') as zip_ref:
                files_in_zip = zip_ref.namelist()

                # Step 9: Create a temporary directory and extract files
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip_ref.extractall(temp_dir)

                    # Step 10: Read datasets from the extracted files
                    datasets = {file: xr.open_dataset(os.path.join(temp_dir, file)) for file in files_in_zip}
                    ds1 = datasets['data_stream-oper_stepType-instant.nc']
                    ds2 = datasets['data_stream-oper_stepType-accum.nc']
                    ds3 = datasets['data_stream-oper_stepType-max.nc']

            # Step 11: Generate daily statistics for surface data
            DS1 = ds1.resample(valid_time='1D').sum()
            DS2 = ds2.resample(valid_time='1D').mean()
            DS3 = ds3[['mx2t']]
            DS4 = ds3[['mn2t']]
            DS5 = DS3.resample(valid_time='1D').max()
            DS6 = DS4.resample(valid_time='1D').min()
            DS1['tp'] = DS2.tp
            DS1['ssrd'] = DS2.ssrd
            DS1['tisr'] = DS2.tisr
            DS1['ttr'] = DS2.ttr
            DS1['mx2t'] = DS5.mx2t
            DS1['mn2t'] = DS6.mn2t
            DS_surface = DS1

            # Step 12: Generate daily statistics for upper air data
            DS7 = ds_upper_air.resample(valid_time='1D').mean()

            # Step 13: Restructure upper air dataset with unique variable names
            DS_upper = xr.Dataset()
            for var in DS7.data_vars:
                for p in DS7.pressure_level:
                    var_at_level = DS7[var].sel(pressure_level=p)
                    var_name = f"{var}_{int(p.values)}hPa"
                    DS_upper[var_name] = var_at_level

            # Step 14: Merge surface and upper air datasets
            combined_ds = xr.merge([DS_surface, DS_upper])

            # Step 15: Save the combined dataset to a file
            output_file = os.path.join(output_dir, f'DS_{year:04d}_{month:02d}.nc')
            combined_ds.to_netcdf(output_file)
            print(f"Processed and saved: {output_file}")

        except Exception as e:
            print(f"Error processing {upper_air_file} and {surface_file}: {e}")

# Step 16: Calculate and print the total runtime
end_time = time.time()
print(f"Total runtime of the program is {end_time - start_time} seconds")
