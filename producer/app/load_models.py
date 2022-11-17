# from flair.models import TextClassifier
from flair.models import SequenceTagger

# classifier = TextClassifier.load('en-sentiment')
tagger = SequenceTagger.load('flair/ner-english')