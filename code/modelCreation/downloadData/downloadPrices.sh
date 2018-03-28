#!/bin/bash
echo $1 $2
mkdir $2
for i in `cat $1 | cut -d"," -f1`
do
    echo $i
    link="http://real-chart.finance.yahoo.com/table.csv?s=$i&d=1&e=1&f=2016&g=d&a=1&b=1&c=2009&ignore=.csv"
    echo $link
    wget $link -O ./$2/$i
done
