FROM python:3.9

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY /models /flair/models
COPY /app /app

RUN python app/load_models.py

ENTRYPOINT [ "python", "app/producer.py", "-f", "app/librdkafka.config"]
