CONTENT=$1
backup1=$2
result=$3
C=$(echo $CONTENT | sed 's/\//\\\//g')
sed "/<\/database>/ s/.*/${C}\n&/" $backup1 > $result
