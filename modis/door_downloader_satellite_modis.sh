#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='DOOR DOWNLOADER - SATELLITE MODIS'
script_version="1.1.0"
script_date='2020/10/30'

virtualenv_folder='/hydro/library/fp_libs_python3/'
virtualenv_name='virtualenv_python3'
script_folder='/hydro/library/hyde/'

# Execution example:
# python3 hyde_downloader_modis_snow.py -settings_file hyde_downloader_satellite_modis.json -time "2020-10-26 03:23"
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Get file information
script_file='/hydro/library/hyde/bin/downloader/modis/hyde_downloader_modis_snow.py'
settings_file='/hydro/fp_tools_preprocessing/satellite/modis/hyde_downloader_satellite_modis_history.json'

# Get information (-u to get gmt time)
# time_now=$(date -u +"%Y-%m-%d %H:00")
time_now='2020-11-01 00:00' # DEBUG
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate virtualenv
export PATH=$virtualenv_folder/bin:$PATH
source activate $virtualenv_name

# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder"

# Libraries related to modis satellite reprojection tool
MRT_HOME="/hydro/library/mrt-4.1"
PATH="$PATH:/hydro/library/mrt-4.1/bin"
MRT_DATA_DIR="/hydro/library/mrt-4.1/data"
export MRT_HOME PATH MRT_DATA_DIR
#-----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."
echo " ==> COMMAND LINE: " python3 $script_file -settings_file $settings_file -time $time_now

# Run python script (using setting and time)
python3 $script_file -settings_file $settings_file -time "$time_now"

# Info script end
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

