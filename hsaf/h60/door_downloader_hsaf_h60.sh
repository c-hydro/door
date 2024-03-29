#!/bin/bash -e

# ----------------------------------------------------------------------------------------
# Script information
script_name='DOOR DOWNLOADER - HSAF PRODUCT PRECIPITATION H60'
script_version="2.4.0"
script_date='2024/02/14'

# script mode
script_mode='history' # 'history' or 'realtime' 
# script period
days=2000

# Script argument(s)
data_folder_static_geo="/share/HSAF_PRECIPITATION/ancillary/geo/h60/"
data_folder_static_grid="/share/HSAF_PRECIPITATION/ancillary/grid/h60/"
data_folder_dynamic_src_raw="/share/HSAF_PRECIPITATION/nrt/h60/%YYYY/%MM/%DD/"
data_folder_dynamic_dst_raw="/share/HSAF_PRECIPITATION/dewetra/h60_corrected_extrapolation/%YYYY/%MM/%DD/%HH00/"

# script ftp settings
proxy="http://130.251.104.8:3128"
#proxy=""

var_rain_in='rr'
var_rain_out='rain_rate'
var_quality_in='qind'
var_quality_out='quality_index'

ftp_machine=""
ftp_url=""
ftp_usr="" 
ftp_pwd=""

# check mode to choose ftp folder
if [ "$script_mode" == 'realtime' ]; then
	ftp_folder_raw="/products/h60/h60_cur_mon_data/" # realtime
elif [ "$script_mode" == 'history' ]; then 
    ftp_folder_raw="/hsaf_archive/h60/%YYYY/%MM/%DD/%HH/" # history
else 
    printf "This program requires 'history' or 'realtime' mode\n" 1>&2
    exit 1
fi 


file_name_nc_geo="lat_lon_0.nc"
file_name_nc_grid="grid_europe_h60"

str_nc_tmp_step1='tmp_fulldisk'
str_nc_tmp_step2='tmp_domain'
str_nc_tmp_step3='tmp_regrid'
str_nc_tmp_step4='tmp_setmissing'
str_nc_out='europe'

list_var_drop='x,y'
var_dynamic_rename_rain=${var_rain_in}','${var_rain_out}
var_dynamic_rename_quality=${var_quality_in}','${var_quality_out}

var_geo_x='long'
var_geo_y='latg'
var_geo_rename_x='long,lon'
var_geo_rename_y='latg,lat'

# sellonlatbox,lon1,lon2,lat1,lat2
domain_bb='-15,30,30,60'

file_out_reinit=0
file_out_compression=1
file_out_zip=0

# server command-line
ncks_exec="/home/idrologia/apps/nco/bin/ncks"
ncrename_exec="/home/idrologia/apps/nco/bin/ncrename"
ncpdq_exec="/home/idrologia/apps/nco/bin/ncpdq"
cdo_exec="/home/idrologia/apps/cdo/bin/cdo"

# local command-line
#ncks_exec="/home/fabio/Desktop/Apps/nco-4.8.0_nc-4.6.0/bin/ncks"
#ncrename_exec="/home/fabio/Desktop/Apps/nco-4.8.0_nc-4.6.0/bin/ncrename"
#ncpdq_exec="/home/fabio/Desktop/Apps/nco-4.8.0_nc-4.6.0/bin/ncpdq"
#cdo_exec="/home/fabio/Desktop/Apps/cdo-2.0.0_nc-4.6.0_hdf-1.8.17_eccodes-2.20.0/bin/cdo"

# Export library path dependecies
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/idrologia/library/eccodes/lib/

# CDO Option(s)  
export REMAP_EXTRAPOLATE=off
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Get time
time_now=$(date '+%Y-%m-%d')
time_now='2024-02-13'
# ----------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------
# Info script start
echo " ==================================================================================="
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> START ..."

# get credentials from .netrc (if not defined in the bash script)
if [[ -z ${ftp_usr} || -z ${ftp_pwd} ]]; then

	# check .netrc file availability
	netrc_file=~/.netrc
	if [ ! -f "${netrc_file}" ]; then
	  echo "${netrc_file} does not exist. Please create it to store login and password on your machine"
	  exit 0
	fi

	# get information from .netrc file
	ftp_usr=$(awk '/'${ftp_machine}'/{getline; print $4}' ~/.netrc)
	ftp_pwd=$(awk '/'${ftp_machine}'/{getline; print $6}' ~/.netrc)

fi
echo " ===> INFO MACHINE -- URL: ${ftp_url} -- USER: ${ftp_usr}"

# Iterate over day(s)
for day in $(seq 0 $days); do
    
    # ----------------------------------------------------------------------------------------
    # Get time step
	date_step=$(date -d "${time_now} -${day} days" +%Y%m%d)
	# ----------------------------------------------------------------------------------------
	
    # ----------------------------------------------------------------------------------------
    # Info time start
    echo " ===> TIME_STEP: "$date_step" ===> START "
	
    # Define time step information
    date_get=$(date -u -d "$date_step" +"%Y%m%d%H")
    doy_get=$(date -u -d "$date_step" +"%j")

    year_get=$(date -u -d "$date_step" +"%Y")
    month_get=$(date -u -d "$date_step" +"%m")
    day_get=$(date -u -d "$date_step" +"%d")
    hour_get=$(date -u -d "$date_step" +"%H")
    # ----------------------------------------------------------------------------------------
	
	# ----------------------------------------------------------------------------------------
	# Iterate over hour(s)
	if [ "$script_mode" == 'realtime' ]; then
		count_start=${hour_get}
		count_end=${hour_get}
	elif [ "$script_mode" == 'history' ]; then 
		count_start=23
		count_end=0
	fi 
	
	# debug
	#count_start=9
	#count_end=9
	
	for hour in $(seq ${count_start} -1 ${count_end}); do
		
		# ----------------------------------------------------------------------------------------
		# get hour 
		if [ "$script_mode" == 'realtime' ]; then
			hour_get=$(printf $(date '+%H'))
		elif [ "$script_mode" == 'history' ]; then 
			hour_get=$(printf "%02d" ${hour})
		fi
		# ----------------------------------------------------------------------------------------
		
    	# ----------------------------------------------------------------------------------------
    	# Info time start
    	echo " ===> HOUR_STEP: "$hour_get" ===> START "

		# Define ftp folder(s)
		ftp_folder_def=${ftp_folder_raw/'%YYYY'/$year_get}
		ftp_folder_def=${ftp_folder_def/'%MM'/$month_get}
		ftp_folder_def=${ftp_folder_def/'%DD'/$day_get}
		ftp_folder_def=${ftp_folder_def/'%HH'/$hour_get}

		# Define dynamic folder(s)
    	data_folder_dynamic_src_def=${data_folder_dynamic_src_raw/'%YYYY'/$year_get}
    	data_folder_dynamic_src_def=${data_folder_dynamic_src_def/'%MM'/$month_get}
    	data_folder_dynamic_src_def=${data_folder_dynamic_src_def/'%DD'/$day_get}    
    	if [ "$script_mode" == 'realtime' ]; then	
			data_folder_dynamic_src_def=${data_folder_dynamic_src_def/'%HH'/'realtime'}
		elif [ "$script_mode" == 'history' ]; then 
			data_folder_dynamic_src_def=${data_folder_dynamic_src_def/'%HH'/$hour_get}
		fi
		# ----------------------------------------------------------------------------------------

		# ----------------------------------------------------------------------------------------	
		# Create folder(s)
		if [ ! -d "$data_folder_dynamic_src_def" ]; then
			mkdir -p $data_folder_dynamic_src_def
		fi
		# ----------------------------------------------------------------------------------------
		
		# ----------------------------------------------------------------------------------------
		# Get file list from ftp
		# Example
		# open -u "sgabellani_r","gabellaniS334" "ftphsaf.meteoam.it"
		# cd "/products/h60/h60_cur_mon_data/"
		# cls -1 | sort -r | grep "20230515" | sed -e "s/@//"
		
		ftp_file_list=`lftp << EOF
		set ftp:proxy ${proxy}
		open -u ${ftp_usr},${ftp_pwd} ${ftp_url}
		cd ${ftp_folder_def}
		cls -1 | sort -r | grep ${date_step} | sed -e "s/@//"
		close
		quit
EOF`
	   	#echo " ===> LIST FILES: $ftp_file_list "
	   	#exit
		# ----------------------------------------------------------------------------------------

		# ----------------------------------------------------------------------------------------
		# Download file(s)	
		for ftp_file in ${ftp_file_list}; do
		    
		    echo -n " ====> DOWNLOAD FILE: ${ftp_file} IN ${data_folder_dynamic_src_def} ..." 
		    
			if ! [ -e ${data_folder_dynamic_src_def}/${ftp_file} ]; then
				
				`lftp << ftprem
					        set ftp:proxy  ${proxy}
							open -u ${ftp_usr},${ftp_pwd} ${ftp_url}
							cd ${ftp_folder_def}
							get1 -o ${data_folder_dynamic_src_def}/${ftp_file} ${ftp_file}
							close
							quit
ftprem`

				if [ $? -eq 0 ] > /dev/null 2>&1; then
			 		echo " DONE!"
				else
					echo " FAILED [FTP ERROR]!"
				fi
			
			else
				echo " SKIPPED! File previously downloaded!"
			fi
		    # ----------------------------------------------------------------------------------------
		    
		done
		# ----------------------------------------------------------------------------------------
		
		# ----------------------------------------------------------------------------------------
		# Iterate on file(s) for converting to netcdf over domain
		for file_name_zip_in in ${ftp_file_list}; do
		    
		    # ----------------------------------------------------------------------------------------
		    # Filename(s) definition(s)
		    file_name_tmp=$(basename -- "$file_name_zip_in")
		    file_ext_tmp="${file_name_tmp##*.}"
		    
		    file_name_nc_in="${file_name_tmp%.*}"
		    
		    file_date_part1_nc_in=${file_name_nc_in:4:8}
		    file_date_part2_nc_in=${file_name_nc_in:13:4}
		    
		    file_year=${file_date_part1_nc_in:0:4}
		    file_month=${file_date_part1_nc_in:4:2}
		    file_day=${file_date_part1_nc_in:6:2}
		    file_hour=${file_date_part2_nc_in:0:2}
		    file_min=${file_date_part2_nc_in:2:4}
		    
		    # echo $file_year $file_month $file_day $file_hour $file_min
		    
		    data_folder_dynamic_dst_def=${data_folder_dynamic_dst_raw/'%YYYY'/$file_year}
			data_folder_dynamic_dst_def=${data_folder_dynamic_dst_def/'%MM'/$file_month}
			data_folder_dynamic_dst_def=${data_folder_dynamic_dst_def/'%DD'/$file_day}
	   	 	data_folder_dynamic_dst_def=${data_folder_dynamic_dst_def/'%HH'/$file_hour}
		    
			if [ ! -d "$data_folder_dynamic_dst_def" ]; then
				mkdir -p $data_folder_dynamic_dst_def
			fi
		    
		    file_name_nc_generic="${file_name_nc_in%.*}"
		    file_name_nc_generic=${file_name_nc_generic/'fdk'/'%DOMAIN'}
		    
		    file_name_nc_tmp_step1=${file_name_nc_generic/'%DOMAIN'/$str_nc_tmp_step1}'.nc'
		    file_name_nc_tmp_step2=${file_name_nc_generic/'%DOMAIN'/$str_nc_tmp_step2}'.nc'
		    file_name_nc_tmp_step3=${file_name_nc_generic/'%DOMAIN'/$str_nc_tmp_step3}'.nc'
		    file_name_nc_tmp_step4=${file_name_nc_generic/'%DOMAIN'/$str_nc_tmp_step4}'.nc'
		    
		    file_name_nc_pattern='hsaf_h60_'${file_date_part1_nc_in}${file_date_part2_nc_in}'_'${str_nc_out}
		    
		    #file_name_nc_out=${file_name_nc_generic/'%DOMAIN'/$str_nc_out}'.nc'
		    #file_name_zip_out=${file_name_nc_generic/'%DOMAIN'/$str_nc_out}'.nc.gz'
		    
		    file_name_nc_out=${file_name_nc_pattern}'.nc'
		    file_name_zip_out=${file_name_nc_pattern}'.nc.gz'
		    
			file_path_zip_in=${data_folder_dynamic_src_def}${file_name_zip_in}
			file_path_nc_in=${data_folder_dynamic_src_def}${file_name_nc_in}
			file_path_nc_tmp_step1=${data_folder_dynamic_dst_def}${file_name_nc_tmp_step1}
			file_path_nc_tmp_step2=${data_folder_dynamic_dst_def}${file_name_nc_tmp_step2}
			file_path_nc_tmp_step3=${data_folder_dynamic_dst_def}${file_name_nc_tmp_step3}
			file_path_nc_tmp_step4=${data_folder_dynamic_dst_def}${file_name_nc_tmp_step4}
			file_path_nc_out=${data_folder_dynamic_dst_def}${file_name_nc_out}
			file_path_zip_out=${data_folder_dynamic_dst_def}${file_name_zip_out}
			
			file_path_nc_geo=${data_folder_static_geo}${file_name_nc_geo}
			file_path_nc_grid=${data_folder_static_grid}${file_name_nc_grid}
			# ----------------------------------------------------------------------------------------
			
		    # ----------------------------------------------------------------------------------------
		    # Flag to check nc file creation
		    create_nc=true

			# Define file check
		    if [ "$file_out_zip" -eq "1" ]; then
				file_path_check_out=${file_path_zip_out}
			else
				file_path_check_out=${file_path_nc_out}
			fi
			
			# Reinit file check
		    if [ "$file_out_reinit" -eq "1" ]; then
				if [ -e ${file_path_check_out} ]; then
					rm -f $file_path_check_out
		    	fi
			fi
		    # ----------------------------------------------------------------------------------------
			
			
			# ----------------------------------------------------------------------------------------
			# Create nc file over selected domain
			echo " ====> CREATE NC FILE: ${file_path_check_out} ..."
			  
			if ! [ -e ${file_path_check_out} ]; then
			
			    # ----------------------------------------------------------------------------------------
			    # Unzip file (from gz to grib)
			    echo -n " =====> UNZIP FILE: ${file_path_zip_in} ..."
			    if ! [ -e ${file_path_nc_in} ]; then
			        if gunzip -k $file_path_zip_in > /dev/null 2>&1; then
		                echo " DONE!"
		            else
		                echo " FAILED! Error in command execution!"
		                create_nc=false
		            fi
				else
					echo " SKIPPED! File previously unzipped!"
				fi
			    # ----------------------------------------------------------------------------------------

			    # ----------------------------------------------------------------------------------------
			    # Reduce file from netcdf to netcdf with dropping varible(s)
			    echo " =====> REDUCE, ADD AND RENAME FILE VARIABLE(S): ${file_path_nc_in} to ${file_path_nc_tmp_step1} ..."
			    if ! [ -e ${file_path_nc_tmp_step1} ]; then
			    
			        if ${ncks_exec} -C -x -v ${list_var_drop} ${file_path_nc_in} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... REDUCE VARIABLE(S) ... DONE!"
		            else
		                echo " ... REDUCE VARIABLE(S) ... FAILED! Error in command execution!"
		                create_nc=false
		            fi
		            
		            if ${ncrename_exec} -h -O -v ${var_dynamic_rename_rain} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... RENAME VAR RAIN RATE ${var_dynamic_rename_rain} ... DONE!"
		            else
		                echo " ... RENAME VAR RAIN RATE ${var_dynamic_rename_rain} ... FAILED! Error in renaming variable!"
		                create_nc=false
		            fi
		            
		            if ${ncrename_exec} -h -O -v ${var_dynamic_rename_quality} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... RENAME VAR RAIN QUALITY ${var_dynamic_rename_quality} ... DONE!"
		            else
		                echo " ... RENAME VAR RAIN QUALITY ${var_dynamic_rename_quality} ... FAILED! Error in renaming variable!"
		                create_nc=false
		            fi
		            
			        if ${ncks_exec} -A -v ${var_geo_x} ${file_path_nc_geo} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... ADD VARIABLE GEO X ... DONE!"
		            else
		                echo " ... ADD VARIABLE GEO X ... FAILED! Error in command execution!"
		                create_nc=false
		            fi
		            
			        if ${ncks_exec} -A -v ${var_geo_y} ${file_path_nc_geo} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... ADD VARIABLE GEO Y ... DONE!"
		            else
		                echo " ... ADD VARIABLE GEO Y ... FAILED! Error in command execution!"
		                create_nc=false
		            fi
		            
		            if ${ncrename_exec} -h -O -v ${var_geo_rename_x} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... RENAME VAR GEO X ${var_geo_rename_x} ... DONE!"
		            else
		                echo " ... RENAME VAR GEO X ${var_geo_rename_x} ... FAILED! Error in renaming variable!"
		                create_nc=false
		            fi
		            
		            if ${ncrename_exec} -h -O -v ${var_geo_rename_y} ${file_path_nc_tmp_step1} > /dev/null 2>&1; then
		                echo " ... RENAME VAR GEO Y ${var_geo_rename_y} ... DONE!"
		            else
		                echo " ... RENAME VAR GEO Y ${var_geo_rename_y} ... FAILED! Error in renaming variable!"
		                create_nc=false
		            fi
		                    
				else
					echo " SKIPPED! File variable(s) previously updated!"
				fi
				# ----------------------------------------------------------------------------------------
			    

			    # ----------------------------------------------------------------------------------------
			    # Select file over domain
			    echo -n " =====> SELECT FILE VARIABLE(S) OVER DOMAIN: ${file_path_nc_tmp_step1} to ${file_path_nc_tmp_step2} ..."
			    if ! [ -e ${file_path_nc_tmp_step2} ]; then
			    
			        if ${cdo_exec} sellonlatbox,${domain_bb} ${file_path_nc_tmp_step1} ${file_path_nc_tmp_step2} > /dev/null 2>&1; then
		                echo " DONE!"
		            else
		                echo " FAILED! Error in command execution!"
		                create_nc=false
		            fi
			        
				else
					echo " SKIPPED! File variable(s) previously selected over domain!"
				fi
			    # ----------------------------------------------------------------------------------------
				
			    # ----------------------------------------------------------------------------------------
			    # Select file over domain
			    echo -n " =====> FLIP FILE VARIABLE(S) OVER DOMAIN: ${file_path_nc_tmp_step2} to ${file_path_nc_tmp_step3} ..."
			    if ! [ -e ${file_path_nc_tmp_step3} ]; then
			    
			        if ${ncpdq_exec} -O -a y,-x -v ${var_rain_out},${var_quality_out} ${file_path_nc_tmp_step2} ${file_path_nc_tmp_step3} > /dev/null 2>&1; then
		                echo " DONE!"
		            else
		                echo " FAILED! Error in command execution!"
		                create_nc=false
		            fi
			        
				else
					echo " SKIPPED! File variable(s) previously flipped!"
				fi
			    # ----------------------------------------------------------------------------------------
				
			    # ----------------------------------------------------------------------------------------
			    # Select file over domain
			    echo -n " =====> REGRID FILE VARIABLE(S) OVER REGULAR GRID: ${file_path_nc_tmp_step3} to ${file_path_nc_tmp_step4} ..."
			    if ! [ -e ${file_path_nc_tmp_step4} ]; then
			    	
			    	if [ "$file_out_compression" -eq "1" ]; then
			    		echo -n " ZIP COMPRESSION ACTIVATED ... " 
			    		
					    if ${cdo_exec} -z zip_9 remapnn,${file_path_nc_grid} ${file_path_nc_tmp_step3} ${file_path_nc_tmp_step4} > /dev/null 2>&1; then
				            echo " DONE!"
				        else
				            echo " FAILED! Error in command execution!"
				            create_nc=false
				        fi
				        
				    else
		        
				    	echo -n " ZIP COMPRESSION NOT ACTIVATED ... "
					    if ${cdo_exec} remapnn,${file_path_nc_grid} ${file_path_nc_tmp_step3} ${file_path_nc_tmp_step4} > /dev/null 2>&1; then
				            echo " DONE!"
				        else
				            echo " FAILED! Error in command execution!"
				            create_nc=false
				        fi
		        
		        	fi
			        
				else
					echo " SKIPPED! File variable(s) previously regridded on regular grid!"
				fi
			    # ----------------------------------------------------------------------------------------
						
			    # ----------------------------------------------------------------------------------------
			    # Select file missing value
			    echo -n " =====> SET FILE VARIABLE(S) MISSING VALUE: ${file_path_nc_tmp_step4} to ${file_path_nc_out} ..."
			    if ! [ -e ${file_path_nc_out} ]; then
			    	
			    	if [ "$file_out_compression" -eq "1" ]; then
			    		echo -n " ZIP COMPRESSION ACTIVATED ... " 
			    		
					    if ${cdo_exec} -z zip_9 setmissval,0 ${file_path_nc_tmp_step4} ${file_path_nc_out} > /dev/null 2>&1; then
				            echo " DONE!"
				        else
				            echo " FAILED! Error in command execution!"
				            create_nc=false
				        fi
				        
				    else
		        
				    	echo -n " ZIP COMPRESSION NOT ACTIVATED ... "
				    	
					    if ${cdo_exec} setmissval,0 ${file_path_nc_tmp_step4} ${file_path_nc_out} > /dev/null 2>&1; then
				            echo " DONE!"
				        else
				            echo " FAILED! Error in command execution!"
				            create_nc=false
				        fi
		        
		        	fi
			        
				else
					echo " SKIPPED! File variable(s) missing values previously set!"
				fi
			    # ----------------------------------------------------------------------------------------

			    # ----------------------------------------------------------------------------------------
			    # Check file nc 
			    echo -n " =====> CHECK AND COMPRESS FILE NC: ${file_path_nc_out} ..."
			    if [ "$file_out_zip" -eq "1" ]; then
					if [ "$create_nc" = true ] ; then
					    gzip ${file_path_nc_out}
					    echo " PASS!"
				    else
				        echo " FAILED! Errors occurred in nc file creation!"
					    if [ -e ${file_path_nc_out} ]; then
					        rm ${file_path_nc_out} 
				        fi
				        create_nc=false
				    fi
				
				else
					echo " SKIPPED! COMPRESSION IS NOT ACTIVATED"
				fi
			    # ----------------------------------------------------------------------------------------
			    
			else
			    # ----------------------------------------------------------------------------------------ù
			    # Exit with no operatio(s)
				echo " ======> SKIPPED! File previously created!"
				# ----------------------------------------------------------------------------------------
			fi  
			# ----------------------------------------------------------------------------------------
			
			# ----------------------------------------------------------------------------------------
			# Remove tmp file(s)
			if [ -e ${file_path_nc_in} ]; then
			    rm ${file_path_nc_in} 
		    fi
		    
			if [ -e ${file_path_nc_tmp_step1} ]; then
			    rm ${file_path_nc_tmp_step1} 
		    fi
			
			if [ -e ${file_path_nc_tmp_step2} ]; then
			    rm ${file_path_nc_tmp_step2} 
		    fi
		    
			if [ -e ${file_path_nc_tmp_step3} ]; then
			    rm ${file_path_nc_tmp_step3} 
		    fi
		    
			if [ -e ${file_path_nc_tmp_step4} ]; then
			    rm ${file_path_nc_tmp_step4} 
		    fi
		    
		    if [ "$file_out_zip" -eq "1" ]; then
				if [ -e ${file_path_nc_out} ]; then
			    	rm ${file_path_nc_out} 
		    	fi
		    fi
			# ----------------------------------------------------------------------------------------
			
			# ----------------------------------------------------------------------------------------
			# Exit message
			if [ "$create_nc" = true ] ; then
		        echo " ====> CREATE NC FILE: ${file_path_check_out} ... DONE!"
		    else
		        echo " ====> CREATE NC FILE: ${file_path_check_out} ... FAILED!"
		    fi
			# ----------------------------------------------------------------------------------------
			
		done

		# ----------------------------------------------------------------------------------------
    	# Info hour end
    	echo " ===> HOUR_STEP: "$hour_get" ===> END "
    	# ----------------------------------------------------------------------------------------
    	
	done
	
	# ----------------------------------------------------------------------------------------
	# Info time end
	echo " ===> TIME_STEP: "$date_step" ===> END "
    # ----------------------------------------------------------------------------------------
	
done

# Info script end
echo " ==> "$script_name" (Version: "$script_version" Release_Date: "$script_date")"
echo " ==> ... END"
echo " ==> Bye, Bye"
echo " ==================================================================================="
# ----------------------------------------------------------------------------------------

