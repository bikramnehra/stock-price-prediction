In this directory we train models and generate TestPredictions

Input from preious pipeline 

1. TrainFeatureVector generated from word2vecFeatureExtraction.py
2. TestFeatureVector generated from word2vecFeatureExtraction-TestData.py


1. TrainWord2VecAvgFeaturesReggressionPerMov.py 

Arguments
$1 -> TrainFeatureVector described above
$2 -> ModelOutputPath 

This models predicts the percentage movement of the stock price

2. TestWord2VecAvgFeaturePerMov.py 

Arguments
$1 -> TrainFeatureVector described above
$2 -> TestFeatureVector described above
$3 -> Model Path of the above model 
$4 -> Saved Test Predictions for visualization 

This models predicts the percentage movement of the stock price

Other Models(Have the same input format as above)

TrainWord2VecAvgFeaturesReggressionACloseNewsOnly.py
TrainWord2VecAvgFeaturesReggressionAClose.py
TrainWord2VecAvgFeaturesReggressionPerMovNewsOnly.py
TrainWord2VecAvgFeaturesReggressionPerMov.py

Similar Test Scripts

TestWord2VecAvgFeatureACloseNewsOnly.py  
TestWord2VecAvgFeatureAClose.py          
TestWord2VecAvgFeaturePerMovNewsOnly.py  
TestWord2VecAvgFeaturePerMov.py          

