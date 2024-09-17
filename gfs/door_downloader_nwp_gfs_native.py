import os
import requests
import xarray as xr
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define parameters
base_url = "https://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.20240305/00/atmos/"
forecast_steps = 10  # Number of forecast steps to download
output_file = "gfs_forecast.nc"  # Output NetCDF file
variables = ['t', 'u', 'v']  # List of variables to include
bounding_box = {'lon_min': -130, 'lon_max': -60, 'lat_min': 20, 'lat_max': 50}  # Bounding box
n_cores = 4  # Number of cores for parallel downloading

# Create a directory to store downloaded files
os.makedirs('gfs_data', exist_ok=True)


def download_file(step):
    file_name = f"gfs.t00z.pgrb2.0p25.f{step:03d}"
    file_url = os.path.join(base_url, file_name)
    local_path = os.path.join('gfs_data', file_name)

    print(f"Downloading {file_name}...")
    response = requests.get(file_url)
    response.raise_for_status()  # Check if the request was successful

    with open(local_path, 'wb') as f:
        f.write(response.content)

    return local_path


# Parallel download using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=n_cores) as executor:
    futures = [executor.submit(download_file, step) for step in range(0, forecast_steps)]
    local_paths = [future.result() for future in as_completed(futures)]

################
#local_paths = [os.path.join('gfs_data', f"gfs.t00z.pgrb2.0p25.f{step:03d}") for step in range(1, forecast_steps)]
################

# Process and combine the GFS files using xarray
print(" --> Read precipitation data...")
with xr.open_mfdataset(local_paths, backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface', 'level': 0, 'stepType':'accum'}},
                       concat_dim='valid_time',
                       data_vars='minimal', combine='nested', coords='minimal', compat='override',
                       engine="cfgrib") as ds:
    # shift longitude and latitude from 0-360 to -180-180
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180)).sortby('longitude')
    ds = ds.where((ds.latitude <= bounding_box["lat_max"]) &
                  (ds.latitude >= bounding_box["lat_min"]) &
                  (ds.longitude >= bounding_box["lon_min"]) &
                  (ds.longitude <= bounding_box["lon_max"]), drop=True)
    out_df = xr.Dataset({"tp": ds["tp"]})
    first_step = out_df["tp"].values[0, :, :]
    out_df["tp"] = out_df["tp"].diff("valid_time", 1)
    out_df["tp"].values[0, :, :] = first_step
    out_df['tp'].attrs['long_name'] = 'precipitation in the time step'
    out_df['tp'].attrs['units'] = 'mm'
    out_df['tp'].attrs['standard_name'] = "precipitation"

print(" --> Read tempertature data...")
with xr.open_mfdataset(local_paths, backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 2}},
                       concat_dim='valid_time',
                       data_vars='minimal', combine='nested', coords='minimal', compat='override',
                       engine="cfgrib") as ds:
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180)).sortby('longitude')
    ds = ds.where((ds.latitude <= bounding_box["lat_max"]) &
                  (ds.latitude >= bounding_box["lat_min"]) &
                  (ds.longitude >= bounding_box["lon_min"]) &
                  (ds.longitude <= bounding_box["lon_max"]), drop=True)
    out_df["2t_C"] = ds["t2m"] - 273.15
    out_df["r2"] = ds["r2"]

print(" --> Read radiation data...")
with xr.open_mfdataset(local_paths, backend_kwargs={'filter_by_keys': {'typeOfLevel': 'surface', 'level': 0, 'stepType':'avg'}},
                       concat_dim='valid_time',
                       data_vars='minimal', combine='nested', coords='minimal', compat='override',
                       engine="cfgrib") as ds:
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180)).sortby('longitude')
    ds = ds.where((ds.latitude <= bounding_box["lat_max"]) &
                  (ds.latitude >= bounding_box["lat_min"]) &
                  (ds.longitude >= bounding_box["lon_min"]) &
                  (ds.longitude <= bounding_box["lon_max"]), drop=True)
    out_df["dswrf"] = ds["dswrf"]

print(" --> Read wind data...")
with xr.open_mfdataset(local_paths, backend_kwargs={'filter_by_keys': {'typeOfLevel': 'heightAboveGround', 'level': 10}},
                       concat_dim='valid_time',
                       data_vars='minimal', combine='nested', coords='minimal', compat='override',
                       engine="cfgrib") as ds:
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180)).sortby('longitude')
    ds = ds.where((ds.latitude <= bounding_box["lat_max"]) &
                  (ds.latitude >= bounding_box["lat_min"]) &
                  (ds.longitude >= bounding_box["lon_min"]) &
                  (ds.longitude <= bounding_box["lon_max"]), drop=True)
    out_df["10wind"] = (ds["u10"] ** 2 + ds["v10"] ** 2) ** (1 / 2)
print(" --> Read tempertature data...")

out_df = out_df.drop(["time","step","surface","heightAboveGround"]).rename({"valid_time": "time", "latitude": "lat", "longitude": "lon"})
# Save combined dataset to NetCDF
print(f"Saving combined dataset to {output_file}...")
out_df.to_netcdf(output_file)

print("Done!")