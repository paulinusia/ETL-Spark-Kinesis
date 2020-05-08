#!/usr/bin/env python
import os
import json
import boto3
import praw 
from config import kinesis_config, reddit_config
# import config.kinesis_config as kinesis_config
# import config.reddit_cred as reddit_cred



stream_name = 'reddit_news'

# Initialize and connect kinesis stream
kinesis_client = boto3.client('kinesis',
                              region_name=kinesis_config.REGION,
                              aws_access_key_id=kinesis_config.ACCESS_KEY,
                              aws_secret_access_key=kinesis_config.SECRET)

# Initialize reddit stream
reddit = praw.Reddit(client_id = reddit_cred.CLIENT_ID, 
                   client_secret = reddit_cred.SECRET,
                   username = reddit_cred.USERNAME,
                   password = reddit_cred.PASSWORD,
                   user_agent = reddit_cred.SECRET_AGENT)

subreddits = reddit.subreddit("worldnews+politics+news+UpliftingNews")

def main():
    for submission in subreddits.stream.submissions():  
        payload = { 'id': str(submission.name),
                    'submission': str(submission.title),
                    'comment_number': int(submission.num_comments),
                    'score':  int(submission.score) }
        
        # RECORD
        try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey= submission.name)
                print('payload success')
        except (AttributeError, Exception) as e:
            print (e)



if __name__ == "__main__":
	main()
