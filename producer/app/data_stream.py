from datetime import datetime
import pandas as pd
import praw
import os
from praw.models.reddit.comment import Comment
from praw.models.reddit.submission import Submission
from praw.models.reddit.subreddit import Subreddit
from flair.data import Sentence
# from flair.models import TextClassifier
from textblob import TextBlob
from flair.models import SequenceTagger

# classifier = TextClassifier.load('en-sentiment')
tagger = SequenceTagger.load('flair/ner-english')

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
PASSWORD = os.environ.get('PASSWORD')
USERNAME = os.environ.get('USERNAME')

def produce_comments_from_stream(subreddit: Subreddit) -> list[Comment]:
    for comment in subreddit.stream.comments(skip_existing=True):
        yield comment

def produce_comments_from_hot_submissions(subreddit: Subreddit) -> list[Comment]:
    for submission in subreddit.hot(limit=100):
        submission: Submission
        for comment in submission.comments.list():
            if not hasattr(comment, 'body'):
                # sometimes a comment is a 'MoreComments' object
                continue
        else:
            yield comment

def produce_data(sub_name) -> list[dict]:
    reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET, password=PASSWORD,
                     user_agent=f'testscript by /u/{USERNAME}', username=USERNAME)

    subreddit: Subreddit = reddit.subreddit(sub_name)

    yield {
        "type": "subreddit",
        "id": subreddit.id if sub_name != "all" else "all",
        "name": sub_name,
    }

    for comment in produce_comments_from_stream(subreddit):
    # for comment in produce_comments_from_hot_submissions(subreddit):
        comment_id = comment.id
        text = comment.body
        submission: Submission = comment.submission

        sentence = Sentence(text)
        # classifier.predict(sentence)
        # sentiment = round(sentence.labels[0].score, 3)
        sentiment = round(TextBlob(text).sentiment[0], 3)
        tagger.predict(sentence)

        yield {
            "type": "submission",
            "id": submission.id,
            "timestamp": int(1000*submission.created_utc),
            "subreddit_id": subreddit.id if sub_name != "all" else "all"
        }

        yield {
            "type": "comment",
            "id": comment_id,
            "timestamp": int(1000*comment.created_utc),
            "submission_id": comment._extract_submission_id(),
            "text": text,
            "sentiment": sentiment,
            # "parent_id": comment.parent().id if type(comment.parent()) is Comment else None,
            # "ups": comment.ups,
            # "downs": comment.downs
        }

        for idx, entity in enumerate(sentence.get_spans('ner')):
            yield {
                "type": "topic",
                "id": f"{comment_id}-{idx}",
                "text": entity.text,
                "comment_id": comment_id
            }

def produce_data_from_csv(repeat=False, thread_id=None) -> list[dict]:
    df = pd.read_csv('app/data.csv')

    for i in range(33):
        for row in df.iterrows():
            data = row[1]

            comment_id = str(data['ID'])
            submission_id = data["Post ID"]
            if repeat:
                text = ""
                sub_name = "p-generated"
                sub_id = f"p-generated-{thread_id}"
                comment_id = f"{sub_id}-{i}-{comment_id}"
                submission_id = f"{sub_id}-{i}-{submission_id}"
            else:
                text = data["Comment Text"]
                sub_name = data["Subreddit"]
                sub_id = data["Subreddit ID"]


            yield {
                "type": "subreddit",
                "id": sub_id,
                "name": sub_name
            }

            yield {
                "type": "submission",
                "id": submission_id,
                "timestamp": int(1000*datetime.timestamp(datetime.now())),
                "subreddit_id": sub_id
            }

            yield {
                "type": "comment",
                "id": comment_id,
                "timestamp": int(1000*datetime.timestamp(datetime.now())),
                "submission_id": submission_id,
                "text": text,
                "sentiment": round(TextBlob(text).sentiment[0], 3),
            }

            if repeat:
                sentence = Sentence("Every Republican politcal ad I got didn't talk about their policies. Only How 'evil' their opponent was")
            else:
                sentence = Sentence(text)
                tagger.predict(sentence)
            for idx, entity in enumerate(sentence.get_spans('ner')):
                yield {
                    "type": "topic",
                    "id": f"{comment_id}-{idx}",
                    "text": entity.text,
                    "comment_id": comment_id
                }
            
        if not repeat:
            break

if __name__ == "__main__":
    for data in produce_data_from_csv(repeat=False):
        print(data)