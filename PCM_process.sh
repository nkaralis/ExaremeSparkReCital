#initial take dir in arg1 and print all xml files

xml_dir=$1
output_dir=$2
echo 'xml directory is: '$xml_dir;

for file in $( find $xml_dir -type f) 
	do
	#echo $file
	b="$(basename $file | rev | cut -d"." -f2-  | rev)"	#take PCM.15
	c="$output_dir/$b.json"

	xml2json -t xml2json $file --strip_text | jq -c '.[].PMC_ARTICLE | .[] | del(.issue, .journalVolume, .journalIssn, .pubType, .pageInfo)' >> $c
	#echo $b
	echo "$file completely processed"
	done 