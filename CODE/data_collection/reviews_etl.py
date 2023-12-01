import requests
import json
import pandas as pd
import time
# import pyspark
# from pyspark.sql.functions import col, isnull
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType

############ INSERT API KEY HERE ############
API_KEY = ''
#############################################

def apiURL(yelp_url):
    global API_KEY
    return f"https://api.scrapingdog.com/scrape?api_key={API_KEY}&url={yelp_url}&dynamic=false"

def apiCall(yelp_url):
    try:
        return requests.get(apiURL(yelp_url), timeout=120).text
    except Exception as e:
        print(f'API connection error detected: {e}')
        return None


def convertToJson(response):
    review_list = json.loads(
        response[response.find('\"review\":[')+9:response.find('],\"aggregateRating\"')+1]
    )
    return review_list

def buildDf(yelp_id, review_list):
    author_list = [r['author'] for r in review_list] 
    datePublished_list = [r['datePublished'] for r in review_list] 
    reviewRating_list = [r['reviewRating']['ratingValue'] for r in review_list] 
    reviewText_list = [r['description'] for r in review_list]
    
    return pd.DataFrame(
        {
            'id': [yelp_id for i in range(len(review_list))],
            'author': author_list,
            'rating': reviewRating_list,
            'text': reviewText_list
        }
    )

def getIdsAndUrls():
    business_10plus_df = pd.read_csv('csv_files/business.csv', usecols = ['id','url'])

    # Truncate URL and remove any businesses where id = #NAME?
    business_10plus_df['url'] = business_10plus_df['url'].apply(lambda u: u[0:u.find('?')])
    business_10plus_df = business_10plus_df[business_10plus_df['id'] != '#NAME?']

    # Read in distinct ids in review.csv
    reviews_df = pd.read_csv('csv_files/reviews.csv', usecols = ['id', 'rating']).drop_duplicates(['id'])

    # Read in ids in review_error.csv
    reviews_error_df = pd.read_csv('csv_files/reviews_error.csv')
    reviews_error_df['error_flag'] = 1

    # Only select ids and urls that don't exist in reviews.csv or review_error.csv
    joined_df = business_10plus_df \
                    .merge(reviews_df, left_on='id', right_on='id', how='left') \
                    .merge(reviews_error_df, left_on='id', right_on='id', how='left')


    joined_df = joined_df[joined_df['rating'].isna() & joined_df['error_flag'].isna()] \
                    .drop(columns=['rating', 'error_flag']) \
                    .sort_values('id') \
                    .reset_index(drop=True)

    # Return list of id-url pairs
    return [joined_df.loc[i,:].values.flatten().tolist() for i in range(0, joined_df.shape[0])]


def main(business_count=10):
    # Counters for final report out
    success_counter = 0
    error_counter = 0
    
    # Get list of ids and urls for API calls
    id_url_list = getIdsAndUrls()
    
    # Read reviews.csv
    reviews_df = pd.read_csv('csv_files/reviews.csv')
    
    # Read reviews_error.csv
    reviews_error_df = pd.read_csv('csv_files/reviews_error.csv')

    for b in id_url_list[0: min(business_count, len(id_url_list))]:
        print(f'START: Collect review data for id {b[0]} ({success_counter+error_counter+1}/{min(business_count, len(id_url_list))})')
        try:    
            response = apiCall(yelp_url=b[1])
            review_list = convertToJson(response=response)
            response_df = buildDf(yelp_id=b[0], review_list=review_list)

            reviews_df = pd.concat([reviews_df, response_df])
            reviews_df.to_csv('csv_files/reviews.csv', index=False)
            print(f'SUCCESS: Review data for id {b[0]} successfully written to CSV')

            success_counter+=1
        except:
            print(f'*** ERROR: Review data not collected for id {b[0]} ***')
            reviews_error_df = pd.concat([reviews_error_df, pd.DataFrame([b[0]], columns=['id'])])
            reviews_error_df.to_csv('csv_files/reviews_error.csv', index=False)

            error_counter+=1
    
    print('################# COMPLETE #################') 
    print(f'API call for {min(business_count, len(id_url_list))} businesses complete')
    print(f'- Successes: {success_counter}  - Errors: {error_counter}')
    print(f'- Remaining businesses to scrape: {len(id_url_list) - min(business_count, len(id_url_list))}')
    print('############################################') 

if __name__ == '__main__':
    main()