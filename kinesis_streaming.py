#!/usr/bin/env python
import os
import json
import boto3
import praw 
from config.config import kinesis_config, reddit_config

stream_name = 'reddit_news'

# Initialize and connect kinesis stream
kinesis_client = boto3.client('kinesis',
                              region_name=kinesis_config.REGION,
                              aws_access_key_id=kinesis_config.ACCESS_KEY,
                              aws_secret_access_key=kinesis_config.SECRET)

# Initialize reddit stream
reddit = praw.Reddit(client_id = reddit_config.CLIENT_ID, 
                   client_secret = reddit_config.SECRET,
                   username = reddit_config.USERNAME,
                   password = reddit_config.PASSWORD,
                   user_agent = reddit_config.SECRET_AGENT)

subreddits = reddit.subreddit("worldnews+politics+news+UpliftingNews")

def main():
    for submission in subreddits.stream.submissions():  
        payload = { 'id': str(submission.name),
                    'submission': str(submission.title),
                    'comment_number': int(submission.num_comments),
                    'score':  int(submission.score) }

        try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey= submission.name)
                print("Payload success:         {}".format(submission.name))

        except (AttributeError, Exception) as e:
            print (e)



if __name__ == "__main__":
	main()
