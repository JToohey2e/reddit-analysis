from flair.data import Sentence
from flair.models import TextClassifier

test_comment = "This is a positive comment!"

classifier = TextClassifier.load('en-sentiment')

sentence = Sentence(test_comment)
classifier.predict(sentence)
sentiment = sentence.labels[0].score

print(test_comment)
print(sentiment)
