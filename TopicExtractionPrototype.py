from flair.data import Sentence
from flair.models import SequenceTagger

test_comment = "I love Trump and Biden!"

tagger = SequenceTagger.load('ner')

sentence = Sentence(test_comment)
tagger.predict(sentence)

print(test_comment)

for entity in sentence.get_spans('ner'):
    print(entity.text)
