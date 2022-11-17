#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from data_stream import produce_data, produce_data_from_csv
import ccloud_lib
from threading import Thread
from multiprocessing import Process

def subreddit_schemas():
		key_schema_string = """
{
	"name": "subreddit_key",
	"namespace": "subreddit",
	"type": "string"
}
"""
		value_schema_string = """
{
	"fields": [
		{
			"name": "name",
			"type": "string"
		}
	],
	"name": "subreddit_value",
	"namespace": "subreddit",
	"type": "record"
}
"""
		return {'key': key_schema_string, 'value': value_schema_string}

def submission_schemas():
		key_schema_string = """
{
	"name": "submission_key",
	"namespace": "submission",
	"type": "string"
}
"""
		value_schema_string = """
{
	"fields": [
		{
			"name": "subreddit_id",
			"type": "string"
		},
		{
			"name": "timestamp",
			"type": [ "null", {
				"type" : "long",
				"logicalType" : "timestamp-millis"
			}]
		}
	],
	"name": "submission_value",
	"namespace": "submission",
	"type": "record"
}
"""
		return {'key': key_schema_string, 'value': value_schema_string}

def comment_schemas():
		key_schema_string = """
{
	"name": "comment_key",
	"namespace": "comment",
	"type": "string"
}
"""
		value_schema_string = """
{
	"fields": [
		{
			"name": "text",
			"type": "string"
		},
		{
			"name": "sentiment",
			"type": "double"
		},
		{
			"name": "submission_id",
			"type": "string"
		},
		{
			"name": "timestamp",
			"type": [ "null", {
				"type" : "long",
				"logicalType" : "timestamp-millis"
			}]
		}
	],
	"name": "comment_value",
	"namespace": "comment",
	"type": "record"
}
"""
		return {'key': key_schema_string, 'value': value_schema_string}

def topic_schemas():
		key_schema_string = """
{
	"name": "topic_key",
	"namespace": "topic",
	"type": "string"
}
"""
		value_schema_string = """
{
	"fields": [
		{
			"name": "text",
			"type": "string"
		},
		{
			"name": "comment_id",
			"type": "string"
		}
	],
	"name": "topic_value",
	"namespace": "topic",
	"type": "record"
}
"""
		return {'key': key_schema_string, 'value': value_schema_string}

def produce(sub_name, repeat=False, thread_id=None):
		# Read arguments and configurations and initialize
		args = ccloud_lib.parse_args()
		config_file = args.config_file
		conf = ccloud_lib.read_ccloud_config(config_file)

		topics = ["subreddit", "submission", "comment", "topic"]
		producers = {}
		serializers = {topic: {'key': None, 'value': None} for topic in topics}

		schemas = {}
		schemas['topic'] = topic_schemas()
		schemas['comment'] = comment_schemas()
		schemas['submission'] = submission_schemas()
		schemas['subreddit'] = subreddit_schemas()
		for topic in topics:
				# Create Producer instance
				schema_url = "http://schema-registry:8081"
				schema_registry_client =  SchemaRegistryClient({"url": schema_url})
				producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)

				for type in ['key', 'value']:
						serializers[topic][type] = AvroSerializer(schema_registry_client, schemas[topic][type])

				producers[topic] = Producer(producer_conf)

				# Create topic if needed
				ccloud_lib.create_topic(conf, topic)

		if repeat:
				iterator = lambda: produce_data_from_csv(repeat, thread_id)
		else:
				iterator = lambda: produce_data(sub_name)

		for data in iterator():
				topic = data.pop('type')
				print(f'{"{0: <12}".format(sub_name)} | {"{0: <12}".format(topic)} | {data}')
				if 'body' in data:
						data.pop('body')
				record_key = data.pop('id')
				producers[topic].produce(
						topic=topic,
						key=serializers[topic]['key'](record_key, SerializationContext(topic, MessageField.KEY)),
						value=serializers[topic]['value'](data, SerializationContext(topic, MessageField.VALUE))
				)
				producers[topic].poll(0)

		for topic, producer in producers.items():
				producer.flush()

if __name__ == "__main__":
		repeat = False

		if repeat:
			for i in range(10):
					Process(target=produce, args=(f'tread-{i}',True,i)).start()
		else:
			# subs = ["gaming", "worldnews", "antiwork", "politics", "pcmasterrace", "movies", "Minecraft"]
			subs = ["Conservative", "democrats"]
			for sub in subs:
					Thread(target=produce, args=(sub,)).start()


