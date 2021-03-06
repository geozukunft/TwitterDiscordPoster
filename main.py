import os
from dotenv import load_dotenv
import tweepy as tw
import pandas as pd
import asyncio
import asyncpg
import json
import requests

load_dotenv()

consumer_key = os.getenv('consumer_key')
consumer_secret = os.getenv('consumer_secret')
access_token = os.getenv('access_token')
access_token_secret = os.getenv('access_token_secret')
webhook_url = os.getenv('webhook_url')
twitter_list = os.getenv('twitter_list')


async def main():
    conn = await asyncpg.create_pool(user=os.getenv('DB_USER'), password=os.getenv('DB_PW'),
                                     database=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                     port=os.getenv('DB_PORT'))

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    tweets = api.list_timeline(list_id=twitter_list,
                               # 200 is the maximum allowed count
                               count=10,
                               include_rts=True,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    tweets_to_send: list = []
    for info in tweets:
        tweets_to_send.append(info)

    tweets_to_send.reverse()

    for info in tweets_to_send:
        row = await conn.fetchrow('SELECT * FROM tweets WHERE tweetid = $1', info.id)
        if row is None:
            await conn.execute('INSERT INTO tweets VALUES ($1)', info.id)
            if hasattr(info, 'retweeted_status'):
                tweettype = "retweeted"
            else:
                tweettype = "tweeted"

            data = "@" + info.author.screen_name + " " + tweettype + " this at " + str(info.created_at) + \
                   ": https://twitter.com/" + info.author.screen_name + "/status/" + info.id_str
            push_data = {
                'content': data,
                'username': info.author.name,
                'avatar_url': info.author.profile_image_url,
            }

            response = requests.post(
                webhook_url, data=json.dumps(push_data),
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code != 200 and response.status_code != 204:
                raise ValueError(
                    'Request to discord returned an error %s, the response is:\n%s'
                    % (response.status_code, response.text)
                )

    # print(f'done with {tweets_to_send}')
    await asyncio.sleep(20)


if __name__ == "__main__":
    import time

    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s

    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
