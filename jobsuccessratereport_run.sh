#!/bin/sh

VOS="UBooNE NOvA DUNE Mu2e"
YESTERDAY=`date --date yesterday +"%F %T"`
TODAY=`date +"%F %T"`

cd /home/sbhat/JobSuccessRateReport

for vo in ${VOS}
do
	echo $vo
	python JobSuccessReport.py -c jobrate.config -E $vo -s "$YESTERDAY" -e "$TODAY" -T template_jobrate.html -d && echo "Sent report for $vo"
done

 

