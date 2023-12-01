import requests
import pandas as pd
import pyspark
from pyspark.sql.functions import col, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

### API key variables ###
API_KEYS = [k.replace('\n', '') for k in open("keys.txt", "r").readlines()][0:1]
KEY = API_KEYS[0]
KEY_COUNTER = 1
TERMINATE = False

### pyspark set up
spark = pyspark.sql.SparkSession.builder.getOrCreate()
sc = spark.sparkContext


### Helper functions

# Replaces API key after hitting call limit and resets KEY
def keyReplacer():
    global API_KEYS, KEY, KEY_COUNTER, TERMINATE
    
    if len(API_KEYS) == 1:
        TERMINATE = True
        raise Exception('All API Keys have reached the call limit. TERMINATE = True')
    else:
        KEY_COUNTER = 0
        API_KEYS = API_KEYS[1:]
        KEY = API_KEYS[0]

# returns API url for business search endpoint
def apiURL(zipcode, offset=0):
    return f"https://api.yelp.com/v3/businesses/search?location={zipcode}&term=restaurants&sort_by=best_match&limit=50&offset={offset}"

# calls API and returns response json
def apiCall(api_key, apiURL):
    try:
        headers = {
            "accept": "application/json",
            "Authorization": f"bearer {api_key}"
        }
        return requests.get(apiURL, headers=headers).json()
    except Exception as e:
        print(f'API connection error detected: {e}')
        return None

# returns schema for business DataFrame
def defineSchema():
    return StructType([
                StructField('id', StringType(), True),
                StructField('alias', StringType(), True),
                StructField('name', StringType(), True),
                StructField('image_url', StringType(), True),
                StructField('url', StringType(), True),
                StructField('review_count', StringType(), True),
                StructField('categories1', StringType(), True),
                StructField('categories2', StringType(), True),
                StructField('categories3', StringType(), True),
                StructField('rating', StringType(), True),
                StructField('latitude', StringType(), True),
                StructField('longitude', StringType(), True),
                StructField('price', StringType(), True),
                StructField('transactions1', StringType(), True),
                StructField('transactions2', StringType(), True),
                StructField('transactions3', StringType(), True),
                StructField('address1', StringType(), True),
                StructField('address2', StringType(), True),
                StructField('address3', StringType(), True),
                StructField('city', StringType(), True),
                StructField('zip_code', StringType(), True),
                StructField('api_search_zip', StringType(), True),
                StructField('api_search_offset', StringType(), True)
    ])

# returns None (instead of error) if list index out of range
def safeListGet(obj, i):
    try:
        return obj[i]
    except IndexError:
        return None
    
# Extracts data from a single restaurant in json object
def extractRowData(obj, search_zip, offset):
    return [
        obj.get('id'),
        obj.get('alias'),
        obj.get('name'),
        obj.get('image_url'),
        obj.get('url'),
        obj.get('review_count'),
        safeListGet(obj.get('categories'), 0).get('alias') if safeListGet(obj.get('categories'), 0) is not None else None,
        safeListGet(obj.get('categories'), 1).get('alias') if safeListGet(obj.get('categories'), 1) is not None else None,
        safeListGet(obj.get('categories'), 2).get('alias') if safeListGet(obj.get('categories'), 2) is not None else None,
        obj.get('rating'),
        obj.get('coordinates').get('latitude'),
        obj.get('coordinates').get('longitude'),
        obj.get('price'),
        safeListGet(obj.get('transactions'), 0),
        safeListGet(obj.get('transactions'), 1),
        safeListGet(obj.get('transactions'), 2),
        obj.get('location').get('address1'),
        obj.get('location').get('address2'),
        obj.get('location').get('address3'),
        obj.get('location').get('city'),
        obj.get('location').get('zip_code'),
        search_zip,
        offset
    ]
    
# returns a pyspark DataFrame containing all restaurant data in a zip code 
def buildZipDF(zipcode, offset=0):
    global KEY, KEY_COUNTER, TERMINATE
    print(f'START: Call business search API for zip code {zipcode}')
    

    try:
        # call API for zipcode for first time 
        response_json = apiCall(
            KEY, 
            apiURL(zipcode)
        )

        if response_json.get('error') is not None:
            keyReplacer()
            response_json = apiCall(
                KEY, 
                apiURL(zipcode)
            )
            
        KEY_COUNTER += 1
        
        
        # create pyspark DataFrame
        df = spark.createDataFrame(
            schema = defineSchema(),
            data = [extractRowData(biz, search_zip=zipcode, offset=offset) for biz in response_json['businesses']]
        )
        
        # The number of businesses within zipcode
        total = response_json['total']
        print(f'*** Total businesses in zip code {zipcode}: {total}')
        print(f'- Called API for zip code {zipcode} with offset of {offset} | API Calls on current key ({KEY[:5]}) = {KEY_COUNTER}')

        for o in range(50, min(1000, total), 50):
            offset = o
            response_json = apiCall(
                KEY,
                apiURL(zipcode, offset=offset)
            )
             
            if response_json.get('error') is not None:
                keyReplacer()
                response_json = apiCall(
                    KEY, 
                    apiURL(zipcode, offset=offset)
                )
            
            KEY_COUNTER += 1
            print(f'- Called API for zip code {zipcode} with offset of {offset} | API Calls on current key ({KEY[:5]}) = {KEY_COUNTER}')

            df = df.union(
                    spark.createDataFrame(
                        schema = defineSchema(),
                        data = [extractRowData(biz, search_zip=zipcode, offset=offset) for biz in response_json['businesses']]
                 )
            )
        print(f'END: All business search API calls for zip code {zipcode} complete')
        return df 

    except Exception as e:
        print(f'ERROR: {e}')
        print(f'- Total API Calls on current key (prefix = {KEY[:5]}): {KEY_COUNTER}')
        return df

def main():
    global KEY, KEY_COUNTER, TERMINATE
    
    cols_dedup = ['id', 'alias', 'name','image_url', 'url', 'review_count', 'categories1', 
                  'categories2', 'categories3', 'rating', 'latitude', 'longitude', 'price', 
                  'transactions1', 'transactions2', 'transactions3', 'address1', 'address2', 'address3', 
                  'city', 'zip_code']
    
    # import existing business.csv
    business_df = spark \
                    .read \
                    .option("header", True) \
                    .csv("csv_files/business.csv")

    # pull zip codes where data is still needed from nyc_zip_borough_neighborhoods_pop.csv
    zip_csv = pd.read_csv("csv_files/nyc_zip_borough_neighborhoods_pop.csv")
    zip_list = list(zip_csv[zip_csv['status'].isnull()]['zip'])
    
    ########## FOR TESTING ##########
    counter = 1
    ########## FOR TESTING ##########


    while TERMINATE is False:
        zipcode = zip_list[0]
        # build DataFrame with all restaurants in zip code
        zip_df = buildZipDF(zipcode)

        # distinct join with existing business.csv data 
        business_df = business_df \
                        .union(zip_df) \
                        .dropDuplicates(subset=cols_dedup) \
                        .withColumn('api_search_zip', col("api_search_zip").cast(IntegerType())) \
                        .withColumn('api_search_offset', col("api_search_offset").cast(IntegerType())) \
                        .orderBy(col('api_search_zip'), col('api_search_offset'))
    
        # write new business_df DataFrame to csv
        business_df.toPandas().to_csv("csv_files/business.csv", header=True, index=False)
        print(f'SUCCESS {counter}: All business search data zip code {zipcode} has been written into business.csv')
        print('-'*90)

        # Mark zip code as complete
        zip_csv.loc[zip_csv['zip'] == zipcode, 'status'] = 'complete'
        zip_csv.to_csv("csv_files/nyc_zip_borough_neighborhoods_pop.csv", header=True, index=False)

        # Update zip_list by removing the completed zip code  
        zip_list = zip_list[1:]

        ########## FOR TESTING ##########
        if counter == 1:
            TERMINATE = True
        counter += 1
        ########## FOR TESTING ##########

    print('')

if __name__ == '__main__':
    main()