Porfolio creator and simulator

Input from previous pipeline

1. LabelsTrain generated from extractLabels.sh
2. Model Test Prediction genreted from different models
3. LabelsTest generated from extractLabels.sh


1. portfolio.py

$1 -> LabelsTrain defined above
$2 -> Model defined above
$3 -> Save Result file name (save portfolio)
$4 -> LabelsTest defined above

This takes the model and the historicl data and generates portfolio for the user.
It also runs a simulation to observe the performance of the portfolio
