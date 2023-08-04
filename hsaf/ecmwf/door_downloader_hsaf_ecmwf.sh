#!/bin/bash -e

# ----------------------------------------------------------------------------------------
# script generic information
script_name='DOOR - PRODUCT SOIL MOISTURE ECMWF-RZSM'
script_version="3.0.0"
script_date='2023/08/03'

# script machine arg(s)
machine_reference="ftphsaf.meteoam.it"
machine_url="ftphsaf.meteoam.it"
machine_usr=""
machine_pwd=""

# scritp data condition(s)
data_reset=true

# script data arg(s)
data_days_list=(
	10 
	10
	10
	10
	10
	10
	10
	10
	10
	10
)

data_description_list=(
	"H14  - Soil Wetness Profile Index in the roots region retrieved by Metop ASCAT surface wetness scatterometer assimilation method - 25 km - data"
	"H14  - Soil Wetness Profile Index in the roots region retrieved by Metop ASCAT surface wetness scatterometer assimilation method - 25 km - auxiliary"
	"H27  - Scatterometer Root Zone Soil Moisture (RZSM) Data Record 16km resolution"
	"H140 - Scatterometer Root Zone Soil Moisture (RZSM) Data Record 16km resolution - Extension"
	"H141 - Scatterometer Root Zone Soil Moisture Climate Data Record 10km resolutiong - GRIB"
	"H141 - Scatterometer Root Zone Soil Moisture Climate Data Record 10km resolutiong - NETCDF"
	"H142 - Scatterometer Root Zone Soil Moisture Climate Data Record 10km resolution - GRIB"
	"H142 - Scatterometer Root Zone Soil Moisture Climate Data Record 10km resolution - NETCDF"
	"H26  - Metop ASCAT NRT Root Zone Soil Moisture Profile Index 10km resolution - GRIB"
	"H26  - Metop ASCAT NRT Root Zone Soil Moisture Profile Index 10km resolution - NETCDF"
)

data_period_list=(
	"2012-07-01 :: NOW"
	"2018-10-04 :: NOW"
	"1992-01-01 :: 2014-12-31"
	"2015-01-01 :: 2016-12-31"
	"1992-01-01 :: 2018-12-31"
	"1992-01-01 :: 2018-12-31"
	"2019-01-01 :: 2021-12-31"
	"2019-01-01 :: 2021-12-31"
	"2021-11-04 :: NOW"
	"2021-11-04 :: NOW"
)

data_name_list=( 
	"h14" 
	"h14"
	"h27" 
	"h140" 
	"h141" 
	"h141"
	"h142" 
	"h142" 
	"h26"
	"h26"
)

data_expected_list=(
	1 
	1
	1
	1
	1
	1
	1
	1
	1
	1
)

data_active_list=(
	true 
	false
	false
	false
	false
	false
	false
	false
	false
	false
)

data_file_src_list=(
	"h14_%YYYY%MM%DD_0000.grib.bz2"
	"t14_%YYYY%MM%DD_0000.grib.bz2"
	"h27_%YYYY%MM%DD00_T1279.grib"
	"h140_%YYYY%MM%DD00.grib"
	"%YYYY%MM%.tar.gz"
	"h141_%YYYY%MM%DD00_R01.nc"
	"%YYYY%MM%.tar.gz"
	"h142_%YYYY%MM%DD00_R01.nc"
	"h26_%YYYY%MM%DD00_TCO1279.grib.bz2"
	"h26_%YYYY%MM%DD00_R01.nc"
)

data_file_dst_list=(
	"h14_%YYYY%MM%DD_0000.grib.bz2"
	"t14_%YYYY%MM%DD_0000.grib.bz2"
	"h27_%YYYY%MM%DD00_T1279.grib"
	"h140_%YYYY%MM%DD00.grib"
	"%YYYY%MM%.tar.gz"
	"h141_%YYYY%MM%DD00_R01.nc"
	"%YYYY%MM%.tar.gz"
	"h142_%YYYY%MM%DD00_R01.nc"
	"h26_%YYYY%MM%DD00_TCO1279.grib.bz2"
	"h26_%YYYY%MM%DD00_R01.nc"
)

# case realtime
data_folder_src_list=( 
	"/products/h14/h14_cur_mon_grib" 
	"/products/h14_auxiliary/" 
	"/products/h27/h27/SMDAS3_%YYYY_T1279/%YYYY%MM/" 
	"/products/h140/h140/SMDAS3_%YYYY_T1279/%YYYY%MM/" 
	"/products/h141/h141/grib/%YYYY/"
	"/products/h141/h141/netCFD4/%YYYY/"
	"/products/h142/h142/grib/%YYYY/"
	"/products/h142/h142/netCFD4/%YYYY/"
	"/products/h26/h26_cur_mon_grib/"
	"/products/h26/h26_cur_mon_nc"
	)
# case history
#data_folder_src_list=( 
#	"/hsaf_archive/h14/%YYYY/%MM/%DD/" 
#	"" 
#	"" 
#	""
#	"" 
#	""
#	""
#	"/hsaf_archive/h26/%YYYY/%MM/%DD/" 
#	"/hsaf_archive/h26/%YYYY/%MM/%DD/" 
#	)

data_folder_dst_list=(
	"$HOME/datasets/source/h14/%YYYY/%MM/%DD/" 
	"$HOME/datasets/source/h14/%YYYY/%MM/%DD/"
	"$HOME/datasets/source/h27_h140/%YYYY/%MM/%DD/" 
	"$HOME/datasets/source/h27_h140/%YYYY/%MM/%DD/" 
	"$HOME/datasets/source/h141_h142/%YYYY/%MM/"
	"$HOME/datasets/source/h141_h142/%YYYY/%MM/%DD/"
	"$HOME/datasets/source/h141_h142/%YYYY/%MM/"
	"$HOME/datasets/source/h141_h142/%YYYY/%MM/%DD/"
	"$HOME/datasets/source/h26/%YYYY/%MM/%DD/"
	"$HOME/datasets/source/h26/%YYYY/%MM/%DD/"
	)

# Script time arg(s)
time_now=$(date '+%Y-%m-%d %H:00')
#time_now='2023-04-23'
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# info script start
echo " ==================================================================================="
echo " ==> "${script_name}" (Version: "${script_version}" Release_Date: "${script_date}")"
echo " ==> START ..."

# get credentials from .netrc (if not defined in the bash script)
if [[ -z ${machine_usr} || -z ${machine_pwd} ]]; then

	# check .netrc file availability
	netrc_file=~/.netrc
	if [ ! -f "${netrc_file}" ]; then
	  echo "${netrc_file} does not exist. Please create it to store login and password on your machine"
	  exit 0
	fi

	# get information from .netrc file
	machine_usr=$(awk '/'${machine_reference}'/{getline; print $4}' ~/.netrc)
	machine_pwd=$(awk '/'${machine_reference}'/{getline; print $6}' ~/.netrc)

fi
echo " ===> INFO MACHINE -- URL: ${machine_url} -- USER: ${machine_usr}"

# parse and check time information
time_data_now=$(date -d "${time_now}" +'%Y%m%d%H%M')
echo " ===> INFO TIME -- TIME: ${time_data_now}"

# iterate over data name(s)
for ((i=0;i<${#data_description_list[@]};++i)); do
	
	# ----------------------------------------------------------------------------------------
	# get data information
    data_folder_src_tmp="${data_folder_src_list[i]}" 
    data_file_src_tmp="${data_file_src_list[i]}" 
	data_folder_dst_tmp="${data_folder_dst_list[i]}" 
	data_file_dst_tmp="${data_file_dst_list[i]}" 
	data_name_step="${data_name_list[i]}"
	data_description_step="${data_description_list[i]}"
	data_period_step="${data_period_list[i]}" 
	data_days_step="${data_days_list[i]}"   
	data_active_step="${data_active_list[i]}"

	# info description and name start
	echo " ====> PRODUCT NAME '"${data_name_step}"' ... "
	echo " ::: PRODUCT DESCRIPTION: ${data_description_step}"
	# ----------------------------------------------------------------------------------------
	
	# ----------------------------------------------------------------------------------------
	# parse data period to extract data start and data end
	data_period_nospace=$(echo $data_period_step | tr -d '[:space:]')

	# extract data start and data end	
	IFS_DEFAULT="$IFS"
	IFS="::"; read data_period_start data_period_end <<< "$data_period_nospace"
	IFS="$IFS_DEFAULT"
	unset IFS_DEFAULT
	# adjust data format
	data_period_start=${data_period_start/':'/''}
	data_period_end=${data_period_end/':'/''}
	
	# time period now
	time_period_now=$(date -d "${time_now}" +'%Y%m%d')
	# time period start
	time_period_start=$(date -d "${data_period_start}" +'%Y%m%d')
	# time period end
	if [ "${data_period_end}" == "NOW" ] ; then
		time_period_end=$(date -d "${time_now}" +'%Y%m%d')
	else
		time_period_end=$(date -d "${data_period_end}" +'%Y%m%d')
	fi
	
	# info time(s)
	echo " ::: PRODUCT PERIOD: ${data_period_step}"
	echo " ::: PRODUCT NOW: ${time_period_now} -- PRODUCT START: ${time_period_start} PRODUCT END: ${time_period_end}"
	# ----------------------------------------------------------------------------------------
	
	# ----------------------------------------------------------------------------------------
	# check the time with the reference product period
	if [[ $time_period_now -ge $time_period_start ]] && [[ $time_period_now -le $time_period_end ]] ; then
	
		# ----------------------------------------------------------------------------------------
		# flag to activate datasets
		if [ "${data_active_step}" = true ] ; then	
			
			# ----------------------------------------------------------------------------------------
			# iterate over days
			for day in $(seq 0 ${data_days_step}); do
				
				# ----------------------------------------------------------------------------------------
				# get time step
				time_data_step=$(date -d "$time_now ${day} days ago" +'%Y%m%d%H%M')
				year_data_step=${time_data_step:0:4}; 
				month_data_step=${time_data_step:4:2}; day_data_step=${time_data_step:6:2}
				hour_data_step='00'

				# info time download start
				echo " =====> TIME DOWNLOAD: "${time_data_step:0:8}" ... "
				# ----------------------------------------------------------------------------------------
				
				# ----------------------------------------------------------------------------------------
				# Define dynamic folder(s)
				data_folder_src_step=${data_folder_src_tmp/'%YYYY'/$year_data_step}
				data_folder_src_step=${data_folder_src_step/'%MM'/$month_data_step}
				data_folder_src_step=${data_folder_src_step/'%DD'/$day_data_step}
				data_folder_src_step=${data_folder_src_step/'%HH'/$hour_data_step}
				
				data_file_src_step=${data_file_src_tmp/'%YYYY'/$year_data_step}
				data_file_src_step=${data_file_src_step/'%MM'/$month_data_step}
				data_file_src_step=${data_file_src_step/'%DD'/$day_data_step}
				data_file_src_step=${data_file_src_step/'%HH'/$hour_data_step}

				data_folder_dst_step=${data_folder_dst_tmp/'%YYYY'/$year_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%MM'/$month_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%DD'/$day_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%HH'/$hour_data_step}
				
				data_file_dst_step=${data_file_dst_tmp/'%YYYY'/$year_data_step}
				data_file_dst_step=${data_file_dst_step/'%MM'/$month_data_step}
				data_file_dst_step=${data_file_dst_step/'%DD'/$day_data_step}
				data_file_dst_step=${data_file_dst_step/'%HH'/$hour_data_step}
				# ----------------------------------------------------------------------------------------

				# ----------------------------------------------------------------------------------------	
				# Create folder(s)
				if [ ! -d "$data_folder_dst_step" ]; then
					mkdir -p $data_folder_dst_step
				fi
				# ----------------------------------------------------------------------------------------
					
				# ----------------------------------------------------------------------------------------
				# remove file (if flag_reset = true)
				if [ "${data_reset}" = true ] ; then
					if [ -e ${data_folder_dst_step}/${data_file_dst_step} ]; then
						rm -rf ${data_folder_dst_step}/${data_file_dst_step}
					fi
				fi	

				# check file exist or not in the destination folder
				if [ -e ${data_folder_dst_step}/${data_file_dst_step} ]; then
					data_flag_download=false
				else
					data_flag_download=true
				fi
				# ----------------------------------------------------------------------------------------
					
				# ----------------------------------------------------------------------------------------
				# flag to activate download
				if [ "${data_flag_download}" = true ] ; then 
					
					# info download file name start
					echo -n " ======> DOWNLOAD FILE: ${data_file_src_step} IN ${data_folder_dst_step}" 
					
					# get download file name
					`lftp <<-EOF
						open -u ${machine_usr},${machine_pwd} ${machine_url}
						cd ${data_folder_src_step}
						get1 -o ${data_folder_dst_step}/${data_file_dst_step} ${data_file_src_step}
						close
						quit
					EOF
					`
					# info download file name end
					if [ $? -eq 0 ] > /dev/null 2>&1; then
				 		echo " ... DONE!"
					else
						echo " ... FAILED! DOWNLOAD ERROR!"
					fi
					
					# info time download end
					echo " =====> TIME DOWNLOAD: "${time_data_step:0:8}" ... DONE "
					
				else
		
					# info time download end
					echo " =====> TIME DOWNLOAD: "${time_data_step:0:8}" ... SKIPPED. FILE DOWNLOAD PREVIOUSLY."
				fi
				# ----------------------------------------------------------------------------------------

			done

			# info name end
			echo " ====> PRODUCT NAME '"${data_name_step}"' ... DONE"
			# ----------------------------------------------------------------------------------------
			
		else
			
			# ----------------------------------------------------------------------------------------
			# info name end
			echo " ====> PRODUCT NAME '"${data_name_step}"' ... SKIPPED. DOWNLOAD IS NOT ACTIVATED."
			# ----------------------------------------------------------------------------------------
			
		fi
		# ----------------------------------------------------------------------------------------
	
	else
		
		# ----------------------------------------------------------------------------------------
		# info name end
		echo " ====> PRODUCT NAME '"${data_name_step}"' ... SKIPPED. TIME NOW NOT IN THE TIME PERIOD"
		# ----------------------------------------------------------------------------------------
	
	fi
	# ----------------------------------------------------------------------------------------
	
done

# info script end
echo " ==> "${script_name}" (Version: "${script_version}" Release_Date: "${script_date}")"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

