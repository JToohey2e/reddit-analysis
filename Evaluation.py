import pandas as pd
import numpy as np

label_db = pd.read_csv("Evaluation Dataset - Topics.csv")
result_db = pd.read_csv("Evaluation Dataset - Results.csv")

label_db['Topic'] = label_db['Topic'].str.lower()
result_db['Topic'] = result_db['topic'].str.lower()


## Topic Extaction F1 Calculation

max_comment_id = max(max(result_db['comment_id']), max(label_db['Comment ID']))

topic_true_pos = topic_false_pos = topic_false_neg = 0

for i in range(1, max_comment_id):
    label_topics = set(label_db[label_db['Comment ID'] == i]['Topic'])
    result_topics = set(result_db[result_db['comment_id'] == i]['Topic'])

    topic_true_pos += len(label_topics.intersection(result_topics))
    topic_false_pos += len(result_topics.difference(label_topics))
    topic_false_neg += len(label_topics.difference(result_topics))

topic_precision = topic_true_pos / (topic_true_pos + topic_false_pos)
topic_recall = topic_true_pos / (topic_true_pos + topic_false_neg)
topic_f1 = 2 * topic_precision * topic_recall / (topic_precision + topic_recall)

print(topic_f1)


## Topic Sentiment MSE Calculation

label_sentiment_db = label_db.groupby("Topic").agg({"Sentiment": "mean"})
label_topics = set(label_sentiment_db.index)
result_sentiment_db = result_db.groupby("Topic").agg({"sentiment": "mean"})
result_topics = set(result_sentiment_db.index)

def standardColumn(column):
    return (column - column.mean()) / column.std()

label_sentiment_db['Standard_Sentiment'] = standardColumn(label_sentiment_db['Sentiment'])
result_sentiment_db['Standard_Sentiment'] = standardColumn(result_sentiment_db['sentiment'])

sentiment_mse = 0

for topic in result_topics:
    if topic in label_topics:
        result_topic_sentiment = result_sentiment_db.loc[topic]['Standard_Sentiment']
        label_topic_sentiment = label_sentiment_db.loc[topic]['Standard_Sentiment']
        sentiment_mse += (result_topic_sentiment-label_topic_sentiment)**2

print(sentiment_mse)
