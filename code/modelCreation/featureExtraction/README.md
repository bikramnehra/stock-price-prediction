This folder focusses on the feature extraction part of the project

Input Files from previous pipeline

1. TrainNews from divideTrainTest.py
2. TestNews from divideTrainTest.py
3. stop_words (need to be uploaded to hdfs not from pipeline)
4. LabelsTrain from extractLabels.py 
5. LabelsTest from extractLabels.py 


Files:

1. word2vec.py 
Arguments
$1 -> TrainNews Folder path as described above
$2 -> model save location
$3 -> dictionary save location 


It trains the word2vec model and save the model
and also stores the dictionary of words and their
word2vec representation


2. word2vecFeatureExtraction.py 
Arguments
$1 -> stop_words path as described above
$2 -> wordFeatures dictionary output from word2vec.py
$3 -> TrainNews path as described above
$4 -> LabelsTrain path as described above
$5 -> output path for the features

In this scripts feature vector are created by joining 30 day news word2vecfeatures and 30 day ohlc data


3. word2vecFeatureExtraction-TestData.py 
Arguments
$1 -> stop_words path as described above
$2 -> wordFeatures dictionary output from word2vec.py
$3 -> TrainNews path as described above
$4 -> TestNews path as described above
$5 -> LabelsTrain path as described above
$6 -> LabelsTest path as described above
$7 -> output path for the features

In this scripts feature vector are created by joining 30 day news word2vecfeatures and 30 day ohlc data. This same as the above script
only difference is that this for test set
