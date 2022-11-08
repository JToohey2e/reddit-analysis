from textblob import TextBlob

while True:
    test_comment = input("\nInput test comment.\n")

    sentence = TextBlob(test_comment)
    sentiment = sentence.sentiment

    print(sentiment)
