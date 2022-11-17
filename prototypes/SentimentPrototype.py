from flair.data import Sentence
from flair.models import TextClassifier

classifier = TextClassifier.load('en-sentiment')

while True:
    test_comment = input("\nInput test comment.\n")

    if test_comment == 'stop':
        break

    sentence = Sentence(test_comment)
    classifier.predict(sentence)
    if sentence.labels[0].to_dict()['value'] == 'POSITIVE':
        sentiment = sentence.labels[0].to_dict()['confidence']
    else:
        sentiment = -sentence.labels[0].to_dict()['confidence']

    print(sentiment)

from textblob import TextBlob

while True:
    test_comment = input("\nInput test comment.\n")
    
    if test_comment == 'stop':
        break

    sentence = TextBlob(test_comment)
    sentiment = sentence.sentiment

    print(sentiment)
