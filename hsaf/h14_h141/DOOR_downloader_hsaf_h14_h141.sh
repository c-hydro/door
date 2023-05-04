#!/bin/bash -e

#-----------------------------------------------------------------------------------------
# Script information
script_name='DRYES DOWNLOADER - HSAF - H14, H141 and H142'
script_version="2.0.0"
script_date='2023/05/04'

virtualenv_folder='/home/fp_virtualenv_python3/'
virtualenv_name='fp_virtualenv_python3_libraries'
script_folder='/home/door/hsaf/h14_h141/'

# Execution example:
# python3 door_downloader_hsaf_h14_h141.py -settings_file door_downloader_hsaf_h141.json -time "2020-11-02 12:00"
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Get file information
script_file='/home/door/hsaf/h14_h141/door_downloader_hsaf_h14_h141.py'
settings_file='/home/door_downloader_hsaf_h141.json'

# Get information (-u to get gmt time)
#time_now=$(date -u +"%Y-%m-%d %H:00")
time_now='2000-05-31 23:20' # DEBUG 
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# Activate virtualenv
export PATH=$virtualenv_folder/bin:$PATH
source activate $virtualenv_name

# Add path to pythonpath
export PYTHONPATH="${PYTHONPATH}:$script_folder"
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

