beginTime=$(date -d "1 minute ago" +%Y%m%d%H%M00)
endTime=$(date +%Y%m%d%H%M00)
today=$(date -d "1 minute ago" +%Y%m%d)
today_time=$(date -d "today" +"%Y-%m-%d %H:%M:%S")
file_path=/opt/bfd/gkxt/getData/data/hcry.csv
from_path=/opt/bfd/XHC_DATA/V_DWB_SK_XHC_RY/$today

function cat_file()
{
filename=$1
local_path=$file_path
flag='xhc_ry'
if [[ $filename == *$flag* ]];
then 
cat  $local/$filename |awk -F '' '{print "身份证号,"$49","$16",检查站核查,检查站核查,"$14",,,,"}' |sed 's/"//g' >>$local_path
fi
}

> $file_path

files=$(ls -lrt $from_path/* |awk '$9 ~ /'.*'/ {ctime="date -d \""$6" "$7" "$8"\" +%Y%m%d%H%M%S"; ctime|getline filetime; if( filetime >= '$beginTime' &&  filetime <= '$endTime') print $9 ; }'|xargs)
if [ "$files" = "" ];
then
   echo "not inc file"
else
   echo "files: "$files
   for filename in $files
   {
        cat_file $filename
   }
fi
