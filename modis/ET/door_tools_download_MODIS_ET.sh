#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='DOOR - TRANSFER DATASETS - HISTORY'
script_version="1.0.0"
script_date='2023/06/15'

# Virtualenv default definition(s)
virtualenv_folder=$HOME/DRYES/envs/
virtualenv_name='dryes_libraries'

# Default script folder(s)
script_folder=$HOME/DRYES/script/
configuration_folder=$script_folder
package_folder=$HOME/DRYES/libraries/dryes/

# Execution example:
# ./door_tools_download_MODIS_ET.sh
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Get file information
script_file=${script_folder}'downloader_MODIS.py'
settings_file=${configuration_folder}'downloader_MODIS_ET_MYD16A2.json'

#-----------------------------------------------------------------------------------------
# Get time information (-u to get gmt time)
# args: -t "%Y-%m-%d"
# you can assign any hour, forced procedure rounds per day then set time 01:00 of the last day
# arguments in the configuration file have priority
time_now=$(date +"%Y-%m-%d 01:00")

#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate virtualenv
export PATH=$virtualenv_folder/bin:$PATH
source activate $virtualenv_name

# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder"
export PYTHONPATH="${PYTHONPATH}:$package_folder"
#-----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."
echo " ==> COMMAND LINE: " python $script_file -settings_file $settings_file -time $time_now

# Run python script (using setting and time)
python $script_file -settings_file $settings_file -time "$time_now"

# Info script end
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> ... END"
# ----------------------------------------------------------------------------------------

echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

