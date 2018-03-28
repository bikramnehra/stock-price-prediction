This descrbes the download process for the project
Executable code and preferably order is described below

######
1. downloadNews.sh

 This shell script downloads the xml news data from google news
 Example execuation is as follows
./downloadNews.sh top100-Sheet1.csv 2014-01-01 2015-01-01 data
top100-Sheet1.csv is csv which contains the symbols for which we want to download the data
2014-01-01 is start date 
2015-01-01 is end date
data is the folder where data need to be stored

Note: Google has max request limit after it which it starts to reject the request, so we have introduced an Halt to tackle this. After 1 hour pause the application continous.
Roughly it takes 1 day to download 1 year of news information.

The script also maintians the current  progress of the folder, that is if 
script is terminated it does not redownloads the data it has already downloaded but continous from the latest file.

Output Format: In data folder for each day and ticker symbol a xml file is created

Note: In this project we used startdate = 2010-01-01 
and enddate 2016-01-01 and it tool 5 days to  download the dataset.

Whole dataset is place at folder /grad/1/jivjots/stockPricePrediction/datadownload/dataCopy
This data is linux directory not in HDFS

######
2. downloadIndex.sh

This shell script is similar to downloadNews.sh. It has the same command line arguments
We used index.csv to download the data

Note: Output directory of downloadNews.sh and downloadIndex.sh should be same.


######
3. rename.sh

downloadIndex.sh is used to download index news of  the index SNP500 but as its ticker symbol is .INX
we use this script to rename .INX prefix to SNP500
it takes only one argument-> folder where data is present

######
4. downloadPrices.sh

This script is used to download OHLC(Open High Low Close) data set from yahoo finance 
It takes 2 arguments
$1 -> csv file of the stocks
$2 -> output directory
Prices are downloaded from 1/1/2009 to 1/1/2016

SNP500 OHLC data is a single file so it is downloaded manually


5. dataRepartion.py

News downloaded above is very huge data and folder contians many files(approx 182500).
Spark takes very large time to process this file so this newsData is 
coverted to pickle file format.

Note: Path are hardcoded in this script and need to be changed before reuse.

