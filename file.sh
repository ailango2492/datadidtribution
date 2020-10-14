#!/bin/ksh
projectid=$1
files=$2
outfile="/home/pp_risk_grs_batch_qa/ailango/output.csv"
for F in $files
do
cdate=$(stat -c %y $F | cut -d' ' -f1)
ctime=$(stat -c %y $F | cut -d' ' -f2)
fsize=$(stat -c %s $F)
lines=$(< $F wc -l) # | awk '{print $1}' #>> $outfile
line=$(head -n 1 $F)
echo "$projectid"'|||'"$cdate"' '"$ctime"'|||'"$lines"'|||'"$F"'|||'"$line"'|||'"$fsize" >> $outfile
done
