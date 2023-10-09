Door
=============================
**Door - Data cOllector and dOwnloadeR** is a set of tools for the download and the preprocessing of meteorological observation and forecast. Data are preprocessed and customized according to standard formats, compliant with the tools developed by CIMA Foundation [1_] and with the international standards in the hydrological and climatological fields.

**Observations**

- USGS - CHIRPS_ (precipitation - daily 0.05°x0.05°)
- JAXA - GSMAP_ gauge now and real time versions (precipitation - 30-mins 0.1°x0.1°)
- EUMETSAT - HSAF_ (several products with several variables and resolutions)
- NASA - IMERG_ early, late and final runs (precipitation - 30-mins 0.1°x0.1°)
- NASA - MODIS_ (several products with several variables and resolutions)
- NASA - SMAP_ (soil moisture - daily 3kmx3km)
- NASA - VIIRS_ (FAPAR, 8-days 500mx500m)
- UCI-CHRS - PERSIANN_ (precipitation - monthly (0.25°x0.25°)
- NOAA - SCaMPR_ (precipitation - 15-mins 0.1°x0.1°)

**Reanalysis**

- ECMWF - ERA5_ Reanalysis from Copernicus Data Store (hourly - 0.25°x0.25°)

**Forecast**

- Canadian Meteorological Center - GDPS_ (3-hourly 0.15°x0.15°)
- DWD - ICON_ (hourly 0.125° x 0.125°)
- ECMWF - ECMWF-OpenData_ High-Resolution single forecast (3-hourly 0.4°x0.4°)
- NOAA - GFS_ from nomads_ and UCAR_ database (3-hourly / hourly 0.25°x0.25°)
- NOAA - GEFS_ (3-hourly probabilistic ensemble 0.25°x0.25°)

Product available
*************************

We are happy if you want to contribute. Please raise an issue explaining what is missing or if you find a bug. We will also gladly accept pull requests against our master branch for new features or bug fixes.

If you want to contribute please follow these steps:

    • fork the one of the Flood-PROOFS repositories to your account;
    • clone the repository, make sure you use "git clone --recursive" to also get the test data repository;
    • make a new feature branch from the repository master branch;
    • add your feature;
    • please include tests for your contributions in one of the test directories;
    • submit a pull request to our master branch.

Authors
*******

All authors involved in the library development for the Flood-PROOFS modelling system are reported in this authors_ file.

License
*******

By accessing or using the Flood-PROOFS modelling system, code, data or documentation, you agree to be bound by the FloodPROOFS license available. See the license_ for details. 

Changelog
*********

All notable changes and bugs fixing to this project will be documented in this changelog_ file.

References
**********
| [1_] CIMA Hydrology and Hydraulics GitHub Repository

.. _license: LICENSE.rst
.. _changelog: CHANGELOG.rst
.. _authors: AUTHORS.rst
.. _GDPS: https://weather.gc.ca/grib/grib2_glb_25km_e.html
.. _CHIRPS: https://www.chc.ucsb.edu/data/chirps
.. _ICON: https://www.dwd.de/EN/research/weatherforecasting/num_modelling/01_num_weather_prediction_modells/icon_description.html
.. _ECMWF-OpenData: https://www.ecmwf.int/en/forecasts/datasets/open-data
.. _ERA5: https://www.ecmwf.int/en/forecasts/dataset/ecmwf-reanalysis-v5
.. _nomads: https://nomads.ncep.noaa.gov/
.. _UCAR: https://data.ucar.edu/
.. _GFS: https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast
.. _GEFS: https://www.ncei.noaa.gov/products/weather-climate-models/global-ensemble-forecast
.. _GSMAP: https://sharaku.eorc.jaxa.jp/GSMaP/
.. _HSAF: https://hsaf.meteoam.it/
.. _IMERG: https://gpm.nasa.gov/data/imerg
.. _MODIS: https://modis.gsfc.nasa.gov/about/
.. _PERSIANN: https://chrsdata.eng.uci.edu/
.. _SCaMPR: https://www.star.nesdis.noaa.gov/smcd/emb/ff/SCaMPR.php