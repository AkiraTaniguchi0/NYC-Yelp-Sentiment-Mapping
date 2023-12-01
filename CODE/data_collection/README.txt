TeamBD (#110)

Description:
This folder contains all code related to collecting business data via the Yelp API and review data via a web scraping API.

Installation:
1. Obtain an API key for Yelp's fusion API. Detailed instructions can be found at https://docs.developer.yelp.com/docs/fusion-authentication
2. Obtain an API key from a web scraping service. For this project the team used Scraping Dog (https://www.scrapingdog.com), but there are alternative services that will also work. Beyond the free trial, these services will require payment so be sure to unsubscribe once data collection is complete.

Execution:
1. business_etl.py
This file is for collecting business data via the Yelp API. Detailed documentation on the business search endpoint and the data points returned is found at https://docs.developer.yelp.com/reference/v3_business_search

Enter API key under KEY variable within file. Alternatively, multiple API keys can be stored and accessed via the 'keys.txt' file and (current code set up). This set up is to allow for data collection of more restaurants as Yelp's API has a daily limit of 500 API calls. Additionally, create a csv file ('csv_files/nyc_zip_borough_neighborhoods_pop.csv' in existing code) containing the zip codes of the area you want to collect.

If everything is configured correctly, the business data from the API will be written to 'business.csv'. Use Jupyter Notebook to help troubleshoot code execution if any issues.

2. reviews_etl.py
This file is for collecting review data via a web scraping API. Note that this file requires a complete 'business.csv', so business data collection must be completed first prior to collecting review data.

Additional files that must be set up prior to executing the code include:
- reviews.csv: Collects the review data from web scraping API
- reviews_error.csv: The web scraping API will sometimes return errors. This file will track which businesses errored out so that the web scraping API is not called for the restaurant again.

After setting up the files and entering the API key under API_KEY variable, reviews_etl.py is ready to run.

To expedite the review data collection process, multiple Jupyter Notebook workers ('reviews_etl_worker<#>.ipynb) can be deployed. Concurrent requests can be sent to the web scraping API by running the Jupyter Notebooks workers simultaneously shortening the time required to scrape review data. The 'business.csv' will need to be manually partitioned, and separate files will need to be created for each worker. Variables in the first cell will need to be configured to run.
