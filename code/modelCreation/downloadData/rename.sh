for i in `ls $1 -a .INX*`
do
    echo $i
    newName=`echo $i|cut -d"-" -f2-10`
    newName=SNP500-$newName
    echo $newName
    mv $1/$i $1/$newName
done
