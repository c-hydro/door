#!/bin/bash -e

# ----------------------------------------------------------------------------------------
# script generic information
script_name='DOOR - PRODUCT SOIL MOISTURE ASCAT-METOP DR'
script_version="3.0.0"
script_date='2023/08/03'

# script machine arg(s)
machine_reference="ftphsaf.meteoam.it"
machine_url="ftphsaf.meteoam.it"
machine_usr=""
machine_pwd=""
# scritp data condition(s)
data_reset=true
data_extension='.nc'

# script data arg(s)
data_expected_list=(
	935
	935
	935 
	935 
	935
	935
	935 
	935 
	935
	931 
	931
	931
	851
	851
)

data_description_list=(
	"H25  - Metop ASCAT SSM DR2015 time series 12.5 km sampling"
	"H108 - Metop ASCAT SSM DR2015 time series 12.5 km sampling extended"
	"H109 - Metop ASCAT SSM DR2016 time series 12.5 km sampling"
	"H110 - Metop ASCAT SSM DR2016 time series 12.5 km sampling extended"
	"H111 - Metop ASCAT SSM DR2017 time series 12.5 km sampling"
	"H112 - Metop ASCAT SSM DR2017 time series 12.5 km sampling extended"
	"H113 - Metop ASCAT SSM DR2018 time series 12.5 km sampling"
	"H114 - Metop ASCAT SSM DR2018 time series 12.5 km sampling extended"
	"H115 - Metop ASCAT Surface Soil Moisture Climate Data Record v5 12.5 km sampling superseded"
	"H116 - Metop ASCAT Surface Soil Moisture Climate Data Record v5 Extension 12.5 km sampling"
	"H117 - Metop ASCAT Surface Soil Moisture Climate Data Record v6 12.5 km sampling superseded"
	"H118 - Metop ASCAT Surface Soil Moisture Climate Data Record v6 Extension 12.5 km sampling"
	"H119 - Metop ASCAT Surface Soil Moisture Climate Data Record v7 12.5 km sampling"
	"H120 - Metop ASCAT Surface Soil Moisture Climate Data Record v7 Extension 12.5 km sampling"
)
data_name_list=( 
	"h25"
	"h108" 
	"h109" 
	"h110"
	"h111" 
	"h112"
	"h113" 
	"h114" 
	"h115" 
	"h116"
	"h117" 
	"h118"
	"h119" 
	"h120"
)

data_active_list=(
	false
	false
	false 
	false
	false
	false
	false
	false
	false
	false
	false
	false
	false
	true
)

# case realtime
data_folder_src_list=( 
	"/products/h25/h25/SM_ASCAT_TS12.5_DR2015/"
	"/products/h108/"  
	"/products/h109/" 
	"/products/h110/"
	"/products/h111/"
	"/products/h112/"  
	"/products/h113/"
	"/products/h114/"  
	"/products/h115/"
	"/products/h116/"
	"/products/h117/"
	"/products/h118/"
	"/products/h119/"
	"/products/h120/"
)

data_folder_dst_list=(
	"$HOME/datasets/source/h25/"
	"$HOME/datasets/source/h108/"
	"$HOME/datasets/source/h109/"
	"$HOME/datasets/source/h110/" 
	"$HOME/datasets/source/h111/"
	"$HOME/datasets/source/h112/" 
	"$HOME/datasets/source/h113/" 
	"$HOME/datasets/source/h114/" 
	"$HOME/datasets/source/h115/"
	"$HOME/datasets/source/h116/"
	"$HOME/datasets/source/h117/"
	"$HOME/datasets/source/h118/"
	"$HOME/datasets/source/h119/"
	"$HOME/datasets/source/h120/"
)
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

# iterate over data name(s)
for ((i=0;i<${#data_description_list[@]};++i)); do
	
	# ----------------------------------------------------------------------------------------
	# get data information
    data_folder_src_step="${data_folder_src_list[i]}" 
	data_folder_dst_step="${data_folder_dst_list[i]}" 
	data_name_step="${data_name_list[i]}"
	data_description_step="${data_description_list[i]}" 
	data_expected_step="${data_expected_list[i]}"
	data_active_step="${data_active_list[i]}"

	# info description and name start
	echo " ====> PRODUCT NAME '"${data_name_step}"' ... "
	echo " 	 ::: PRODUCT DESCRIPTION: ${data_description_step}"
	# ----------------------------------------------------------------------------------------
	
	# ----------------------------------------------------------------------------------------
	# flag to activate datasets
	if [ "${data_active_step}" = true ] ; then	
		
		# ----------------------------------------------------------------------------------------	
		# Create folder(s)
		if [ ! -d "$data_folder_dst_step" ]; then
			mkdir -p $data_folder_dst_step
		fi
		# ----------------------------------------------------------------------------------------
				
		# ----------------------------------------------------------------------------------------
		# Check download activation
		cd ${data_folder_dst_step}
		data_found_step=$(ls -1 | wc -l)

		if [ "${data_found_step}" -eq "${data_expected_step}" ];then
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
			echo " =====> GET FILE(S) ... ACTIVATED."
			echo "  ::: FILE(S) FOUND (N_FOUND = ${data_found_step}) ARE LESS THEN EXPECTED FILE(S) (N_EXP = ${data_expected_step}) ! "
			
			# get download file list
			data_file_list=`lftp <<-EOF
				set ftp:proxy ${machine_proxy}
				open -u ${machine_usr},${machine_pwd} ${machine_url}
				cd ${data_folder_src_step}
				cls -1 | sort -r | grep ${data_extension} | sed -e "s/@//"
				close
				quit
			EOF
			`
			# ----------------------------------------------------------------------------------------
			
			# ----------------------------------------------------------------------------------------
			# check data file name
			if [ -z "${data_file_list}" ]; then
				# info download file name
				echo " =====> DOWNLOAD FILE(S) ... SKIPPED! FILE(S) ARE NOT AVAILABLE!"
			else

				# iterate over file name list
				for data_file_name in ${data_file_list}; do
					
					# info download file name start
					echo -n " =====> DOWNLOAD FILE: ${data_file_name} IN ${data_folder_dst_step}" 
					
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
			echo " =====> GET FILE(S) ... SKIPPED."
			echo "  ::: ALL EXPECTED FILES (N_EXP = ${data_found_step}) PREVIOUSLY DOWNLOADED!"
			# ----------------------------------------------------------------------------------------
		fi
				
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

