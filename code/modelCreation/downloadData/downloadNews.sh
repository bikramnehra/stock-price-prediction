#!/bin/bash
#./downloadNews.sh tempCsv 2014-01-01 2015-01-01 data
mkdir $4
echo $2 $3
endDate=`date -d $3 +%s`
echo $endDate
HaltCount=0
for i in `cat $1 | cut -d"," -f1`
do
    echo $i
    dayOffset=0
    startDate=`date -d "$2 $dayOffset days" +%s`
    echo $startDate
    while [ $startDate -le $endDate ]
    do
        dateArg=`date -d "$2 $dayOffset days" +%Y-%m-%d`
        fileName=`echo $4/$i-$dateArg.xml`
        echo $fileName
        if [ ! -f $fileName ]; then
            wget "https://www.google.ca/finance/company_news?q=NASDAQ%3A$i&startdate=$dateArg&enddate=$dateArg&output=rss&start=0&num=100" -O $fileName
        fi
        zeroCount=`find $fileName -size  0  | wc -l`
        if [ $zeroCount -gt 0 ]
        then
            echo Halt
            sleep 3600
            HaltCount=$(( HaltCount + 1))
            find . -size  0 -print0 |xargs -0 rm
            if [ $HaltCount -gt 2 ]
            then
                exit
            fi
        else 
            HaltCount=0
            dayOffset=$(( dayOffset + 1 ))
            startDate=`date -d "$2 $dayOffset days" +%s`
        fi
    done
done
