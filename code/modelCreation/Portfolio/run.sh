rm -r Portfolio

$SPARK_HOME/bin/spark-submit portfolio.py ./LabelsTrain ./Model3 ./Portfolio ./LabelsTest
