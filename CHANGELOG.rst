=========
Changelog
=========

Version 1.0.6 [2023-08-04]
**************************
APP: **door_downloader_hsaf_ecmwf.sh**
    - Development of new app to download the hsaf data ECMWF (25km, 16km and 10km) from the hsaf ftp
    
APP: **door_downloader_hsaf_ascat_dr.sh**
    - Development of new app to download the hsaf data record ASCAT (25km and 12.5km) from the hsaf ftp

APP: **door_downloader_hsaf_ascat_nrt.sh**
    - Development of new app to download the hsaf data nrt ASCAT (25km and 12.5km) from the hsaf ftp

APP: **door_downloader_hsaf_h60.sh**
	- Fix the credential procedure using .netrc file;

APP: **door_downloader_hsaf_h61.sh**
	- Fix the credential procedure using .netrc file;
    
Version 1.0.5 [2023-06-30]
**************************
APP: **door_downloader_ecmwf_opendata_single_hires.py**
    - Development of new app to download the open data ECMWF 0.4° open data global forecast

APP: **door_downloader_satellite_chirps.py**
    - Development of new app to download CHIRPS at daily or monthly scale

APP: **door_downloader_nwp_icon.py**
    - Development of new app to download dwd-icon 0.125 global forecast

APP: **door_downloader_nwp_cmc-gdps.py**
    - Development of new app to download CMC Global Deterministic Forecast System (GDPS) 0.15 global forecast

Version 1.0.4 [2023-05-18]
**************************
APP: **door_downloader_hsaf_h60.sh**
	- Development of new app to download and adapt the H60 product to the output file grid
	  using CDO and NCO applications;

APP: **door_downloader_hsaf_h61.sh**
	- Development of new app to download and adapt the H61 product to the output file grid
	  using CDO and NCO applications;

APP: **door_downloader_satellite_imerg.py**
    - Update imerg version for real time use

Version 1.0.3 [2023-05-04]
**************************
APP: **door_downloader_hsaf_h14_h141.py**
	- Development of new app to download and resample HSAF H14, H141, and H142 products

GENERAL:
	- Reorganization of the HSAF directories 

Version 1.0.2 [2023-03-31]
**************************
APP: **door_downloader_satellite_modis.py**
    - Refactor for door package (ET and SNOW examples)

APP: **door_downloader_nwp_gfs_opendap.py**
    - Beta release for door package
    - Alternative nomads downloader for single-processing download

Version 1.0.1 [2022-12-02]
**************************
APP: **door_downloader_satellite_scampr.py**
    - Beta release for door package

UPD: **door_downloader_reanalysis_era5_copernicus.py**
    - Add support for mixed ERA5 and ERA5T downloads
    
Version 1.0.0 [2021-11-30]
**************************
APP: **door_downloader_satellite_gsmap_gauge_historical.py**
    - Simplified version for batch gsmap downloads

APP: **door_downloader_reanalysis_era5_copernicus.py**
    - Beta release for door package

APP: **door_downloader_satellite_imerg.py**
    - Beta release for door package

	   - Previous version(s)
		  - [2021-08-01] Latest update as part of the fp-hyde 1.9.8 package

