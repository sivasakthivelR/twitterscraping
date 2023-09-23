# Import the modules
import snscrape.modules.twitter as sntwitter
import streamlit as st
import pandas as pd
import pymongo
from pymongo import MongoClient
import json

# MongoDB client connection
client = pymongo.MongoClient("mongodb://localhost:27017")
twtdb = client.hemanth
twtdb_main = twtdb.twitterscraping


def main():
    tweets = 0
    st.title("Twitter Scraping")
    menu = ["Search", "Display", "Download"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Search":
        # Every time after the last tweet the database will be cleared for updating new scraping data
        twtdb_main.delete_many({})

        # Form for collecting user input for twitter scrape
        with st.form(key='form1'):
            # Hashtag input
            st.subheader("Tweet searching Form")
            st.write("Enter the hashtag or keyword ")
            query = st.text_input('Hashtag or keyword')

            # No of tweets for scraping
            st.write("Enter the limit for the data scraping: Maximum limit is 1000 tweets")
            limit = st.number_input('Insert a number', min_value=0, max_value=1000, step=10)

            # From date to end date for scraping
            st.write("Enter the Starting date to scrap the tweet data")
            start = st.date_input('Start date')
            end = st.date_input('End date')

            # Submit button to scrap
            submit_button = st.form_submit_button(label="Tweet Scrap")

        if submit_button:
            st.success(f"Tweet hashtag {query} received for scraping".format(query))

            # TwitterSearchScraper will scrape the data and insert into MongoDB database
            for tweet in sntwitter.TwitterSearchScraper(f'from:{query} since:{start} until:{end}').get_items():
                # To verify the limit if condition is set
                if tweets == limit:
                    break
                # Stores the tweet data into MongoDB until the limit is reached
                else:
                    new = {"date": tweet.date, "user": tweet.user.username, "url": tweet.url,
                           "ReplyCount": tweet.replyCount,
                           "Language": tweet.lang, "LikeCount": tweet.likeCount, "Tweet ID": tweet.id,
                           "Text": tweet.content
                           }
                    twtdb_main.insert_one(new)
                    tweets += 1

            # Display the total tweets scraped
            df = pd.DataFrame(list(twtdb_main.find()))
            cnt = len(df)
            st.success(f"Total number of tweets scraped for the input query is := {cnt}".format(cnt))


    elif choice == "Display":
        # Save the documents in a dataframe
        df = pd.DataFrame(list(twtdb_main.find()))
        # Dispaly the document
        st.dataframe(df)

        # Download the scraped data as CSV or JSON
    else:
        # Download the scraped data as CSV

        st.write("Download the tweet data as CSV File")
        # save the documents in a dataframe
        df = pd.DataFrame(list(twtdb_main.find()))
        # Convert dataframe to csv
        df.to_csv('twittercsv.csv')

        def convert_df(data):
            # Cache the conversion to prevent computation on every rerun
            return df.to_csv().encode('utf-8')

        csv = convert_df(df)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='twtittercsv.csv',
            mime='text/csv',
        )
        st.success("Successfully Downloaded data as CSV")

        # Download the scraped data as JSON

        st.write("Download the tweet data as JSON File")
        # Convert dataframe to json string instead as json file
        twtjs = df.to_json(default_handler=str).encode()
        # Create Python object from JSON string data
        obj = json.loads(twtjs)
        js = json.dumps(obj, indent=4)
        st.download_button(
            label="Download data as JSON",
            data=js,
            file_name='twitterjson.js',
            mime='text/js',
        )
        st.success("Successfully Downloaded data as JSON")


# Calling the main function
main()