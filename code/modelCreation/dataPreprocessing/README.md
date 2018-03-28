In this subdirectory we clean data for use
Also we seperate the training and test set
Also we create a Label file with targets

#Inputs from previous layers
1. Pickle file of news generated from repartion.py from datadownload dir
2. csv OHLC data folder downloaded from yahoo finance

Files

1. cleanData.py 

Arg 1 -> input pickle file
Arg 2 -> output pickle file

This takes in the pickle file mentioned above 
removes the html and css tags save the result 
to outputDirectory

see cleanData.sh for example usage

2. divideTrainTest.py

Arg1 -> output of cleanData.py

It divides the dataset in TrainSet, TestSet and writes the result
into TrainNews TestNews folders

3. extractLabels.py

Arg1 -> csv stockData as mentioned above

It calculates the percentage movement of stock prices and divides it into train and test set

It saves the result into LabelsTrain LabelsTest
