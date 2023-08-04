#!/bin/bash -e

# ----------------------------------------------------------------------------------------
# script generic information
script_name='DOOR - PRODUCT SOIL MOISTURE ASCAT-METOP NRT (A+B+C)'
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
	2 
	2 
	2 
	2 
	2 
	2
)

data_hourly_expected_list=(
	20 
	20 
	20 
	20 
	20 
	20
)

data_description_list=(
	"H16  - Metop-B ASCAT NRT SSM orbit geometry 12.5 km sampling"
	"H101 - Metop-A ASCAT NRT SSM orbit geometry 12.5 km sampling"
	"H102 - Metop-A ASCAT NRT SSM orbit geometry 25 km sampling"
	"H103 - Metop-B ASCAT NRT SSM orbit geometry 25 km sampling"
	"H104 - Metop-C ASCAT NRT SSM 12.5 km sampling"
	"H105 - Metop-C ASCAT NRT SSM 25 km sampling"
)
data_name_list=( 
	"h16" 
	"h101" 
	"h102" 
	"h103" 
	"h104" 
	"h105"
)

data_active_list=(
	false 
	false
	false
	false
	true
	false
)

# case realtime
data_folder_src_list=( 
	"/products/h16/h16_cur_mon_data/" 
	"/products/h101/h101_cur_mon_data/" 
	"/products/h102/h102_cur_mon_data/" 
	"/products/h103/h103_cur_mon_data/"
	"/products/h104/h104_cur_mon_data/"
	"/products/h105/h105_cur_mon_data/"
	)
# case history
#data_folder_src_list=( 
#	"/hsaf_archive/h16/%YYYY/%MM/%DD/%HH/" 
#	"/hsaf_archive/h101/%YYYY/%MM/%DD/%HH/" 
#	"/hsaf_archive/h102/%YYYY/%MM/%DD/%HH/" 
#	"/hsaf_archive/h103/%YYYY/%MM/%DD/%HH/"
#	"/hsaf_archive/h104/%YYYY/%MM/%DD/%HH/" 
#	"/hsaf_archive/h105/%YYYY/%MM/%DD/%HH/"  
#	)

data_folder_dst_list=(
	"$HOME/datasets/source/h16/%YYYY/%MM/%DD/%HH/" 
	"$HOME/datasets/source/h101/%YYYY/%MM/%DD/%HH/" 
	"$HOME/datasets/source/h102/%YYYY/%MM/%DD/%HH/" 
	"$HOME/datasets/source/h103/%YYYY/%MM/%DD/%HH/"
	"$HOME/datasets/source/h104/%YYYY/%MM/%DD/%HH/"
	"$HOME/datasets/source/h105/%YYYY/%MM/%DD/%HH/"
	)

# Script time arg(s)
time_now=$(date '+%Y-%m-%d %H:00')
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
	data_folder_dst_tmp="${data_folder_dst_list[i]}" 
	data_name_step="${data_name_list[i]}"
	data_description_step="${data_description_list[i]}"
	data_days_step="${data_days_list[i]}"   
	data_hourly_expected_step="${data_hourly_expected_list[i]}"
	data_active_step="${data_active_list[i]}"

	# info description and name start
	echo " ====> PRODUCT NAME '"${data_name_step}"' ... "
	echo " 	 ::: PRODUCT DESCRIPTION: ${data_description_step}"
	# ----------------------------------------------------------------------------------------
	
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
			
			time_data_check=${year_data_step}${month_data_step}${day_data_step}"0000"
			
			# check the reference hour to consider current and old day(s)
			if [ ${time_data_check:0:8} -ge ${time_data_now:0:8} ]; then
				hour_data_ref=${time_data_step:8:2}
			else
				hour_data_ref="23"
			fi  
			
			# info time day start
			echo " =====> TIMESTEP DAY: "${time_data_step:0:8}" ... "
			# ----------------------------------------------------------------------------------------
			
			# ----------------------------------------------------------------------------------------
			# iterate over hours
			for hour_data_step in $(seq -w ${hour_data_ref} -1 0); do
				
				# ----------------------------------------------------------------------------------------
				# set time donwload
				time_download_step=${year_data_step}${month_data_step}${day_data_step}_${hour_data_step}		

				# info time download start
				echo " ======> TIMESTEP DOWNLOAD: "${time_download_step}" ... "
				# ----------------------------------------------------------------------------------------

				# ----------------------------------------------------------------------------------------
				# Define dynamic folder(s)
				data_folder_src_step=${data_folder_src_tmp/'%YYYY'/$year_data_step}
				data_folder_src_step=${data_folder_src_step/'%MM'/$month_data_step}
				data_folder_src_step=${data_folder_src_step/'%DD'/$day_data_step}
				data_folder_src_step=${data_folder_src_step/'%HH'/$hour_data_step}

				data_folder_dst_step=${data_folder_dst_tmp/'%YYYY'/$year_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%MM'/$month_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%DD'/$day_data_step}
				data_folder_dst_step=${data_folder_dst_step/'%HH'/$hour_data_step}
				# ----------------------------------------------------------------------------------------

				# ----------------------------------------------------------------------------------------	
				# Create folder(s)
				if [ ! -d "$data_folder_dst_step" ]; then
					mkdir -p $data_folder_dst_step
				fi
				# ----------------------------------------------------------------------------------------
				
				# ----------------------------------------------------------------------------------------
				# Check download activation
				cd ${data_folder_dst_step}
				data_hourly_found=$(ls -1 | wc -l)

				if [ "${data_hourly_found}" -eq "${data_hourly_expected_step}" ];then
					data_flag_download=false
				else
					data_flag_download=true
				fi
				# ----------------------------------------------------------------------------------------
				
				# ----------------------------------------------------------------------------------------
				# flag to activate download
				if [ "${data_flag_download}" = true ] ; then 
					
					# ----------------------------------------------------------------------------------------
					# info download activation
					echo " =======> GET FILE(S) ... ACTIVATED."
					echo "  ::: FILE(S) FOUND (N_FOUND = ${data_hourly_found}) ARE LESS THEN EXPECTED FILE(S) (N_EXP = ${data_hourly_expected_step}) ! "
					
					# get download file list
					data_file_list=`lftp <<-EOF
						set ftp:proxy ${machine_proxy}
						open -u ${machine_usr},${machine_pwd} ${machine_url}
						cd ${data_folder_src_step}
						cls -1 | sort -r | grep ${time_download_step} | sed -e "s/@//"
						close
						quit
					EOF
	   				`
					# ----------------------------------------------------------------------------------------
					
					# ----------------------------------------------------------------------------------------
					# check data file name
					if [ -z "${data_file_list}" ]; then
						# info download file name
						echo " =======> DOWNLOAD FILE(S) ... SKIPPED! FILE(S) ARE NOT AVAILABLE!"
					else

						# iterate over file name list
						for data_file_name in ${data_file_list}; do
							
							# info download file name start
							echo -n " =======> DOWNLOAD FILE: ${data_file_name} IN ${data_folder_dst_step}" 
							
							# remove file (if flag_reset = true)
							if [ "${data_reset}" = true ] ; then
								if [ -e ${data_folder_dst_step}/${data_file_name} ]; then
									rm -rf ${data_folder_dst_step}/${data_file_name}
								fi
							fi	
							
							# check file exist or not in the destination folder
							if ! [ -e ${data_folder_dst_step}/${data_file_name} ]; then
								
								# get download file name
								`lftp <<-EOF
									open -u ${machine_usr},${machine_pwd} ${machine_url}
									cd ${data_folder_src_step}
									get1 -o ${data_folder_dst_step}/${data_file_name} ${data_file_name}
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
							
							else
								# info download file name end
								echo " ... SKIPPED! FILE PREVIOUSLY DOWNLOADED!"
							fi
						
						done

					fi
					# ----------------------------------------------------------------------------------------
					
				else
					# ----------------------------------------------------------------------------------------
					# Info about download activation
					echo " =======> GET FILE(S) ... SKIPPED."
					echo "  ::: ALL EXPECTED FILES (N_EXP = ${data_hourly_found}) PREVIOUSLY DOWNLOADED!"
					# ----------------------------------------------------------------------------------------
				fi
				
				# info time download end
				echo " ======> TIMESTEP DOWNLOAD: "${time_download_step}" ... DONE"
				# ----------------------------------------------------------------------------------------

			done

			# info time day end
			echo " =====> TIMESTEP DAY: "${time_data_step:0:8}" ... DONE"
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
	
done

# info script end
echo " ==> "${script_name}" (Version: "${script_version}" Release_Date: "${script_date}")"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

