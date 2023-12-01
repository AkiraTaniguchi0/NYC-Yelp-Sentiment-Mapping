TeamBD (#110)

Description:
This suite of files guides users through the process of creating our project. First, we pulled Yelp Business data from their API. Separately, we had to web scrape Yelp reviews and have included those files here. Next, we have a file that describes how we pre-processed and cleaned the original raw files. From there, we used Google Colab to test and compare multiple natural language processing models in order to extract review sentiment. After that, the team completed feature selection for our clustering algorithm that grouped NYC restaurants based on geography, sentiment, and other features. Finally, we include code that summarizes cluster level data and our entire Tableau workbook.

Installation:
1. data_colletion
Data collection will require API keys from Yelp and a data scraping service. Detailed installation instructions are found in the README.txt file within the data_collection folder
2. DataPreprocessing.ipynb
Ensure that pandas, nltk, re, contractions, and time packages are downloaded. 
3. nlp_models.ipynb
Create a free Google Colab account, link your free Google Drive account to Colab in order to export csv files, and install package transformers to run Huggingface models.
4. Clustering.ipynb
Ensure you have sklearn cluster downloaded, including KMeans, DBSCAN, and AgglomerativeClustering. In addition, download sklearn's decomposition, metrics, datasets, neighbors, and preprocessing. Other packages include numpy, pandas, matplotlib, seaborn, itertools, tqdm, collections, kneed, collections, folium, and re.
5. Tableau (data_visualization)
Requires Tableau desktop to build live connections with data files and make edits to the dashboard

Execution:
1. data_collection Folder
This folder contains code related to collecting business data via the Yelp API and review data via a webscraping API. Detailed instructions on how to execute data collection is found in the README.txt file within the data_collection folder
2. DataPreprocessing.ipynb
This python notebook contains the code to conduct text preprocessing to prepare the raw text reviews before running our NLP models. Before running, ensure that all required packages are downloaded and manually input the name of the csv file that you wish to preprocess in the code itself.
3. nlp_models.ipynb
Running this file on Google Colab, a free Jupyter Notebook program, allowed the team to experiment with multiple pre-trained natural language processing algorithms hosted on the open-source platform Huggingface.co. Simply install the packages, upload a csv file with review data, and test out and comparemultiple models. 
4. Business dataset EDA.ipynb
This python notebook contains EDA on business dataset. This is not mandatory to run but it provides detail steps and reasoning on data cleaning and feature selection.
5. Clustering.ipynb
This python notebook contains detail steps for clustering. Includes reading, cleaning, and manipulating data, different scaling approach for input data, and the various clustering models with respective tuning methods. Before running this notebook, please ensure you have all the required packages downloaded as well as having the input data in the correct folder. 
6. cluster_summary_stats.ipynb
Run this python notebook to get some summary stats required for the cluster summary data visualization page.
7. data_visualization Folder 
This folder contains the final Tableau workbook, data extract, and data files required for a live Tableau connection. The final dashboard is published on Tableau public (https://public.tableau.com/app/profile/jay.nonaka/viz/RestaurantReviewSentimentDashboard/MainDash?publish=yes). Detailed instructions on how to set up the Tableau visualization is found in the README.txt file within the data_visualization folder
