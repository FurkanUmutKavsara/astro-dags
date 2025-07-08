from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow import DAG, Dataset
from airflow.decorators import task
import trafilatura

from airflow.models import Variable
from kafka import KafkaConsumer, KafkaProducer
import json
from trafilatura import fetch_url, extract

server1 = Variable.get("kafka-1")
server2 = Variable.get("kafka-2")
server3 = Variable.get("kafka-3")

producer = KafkaProducer(bootstrap_servers=[server1,server2,server3],value_serializer= lambda x: json.dumps(x).encode('utf-8'))

from datetime import date, datetime

# news_links = Dataset("/tmp/news_links.txt")



def webScrape(message):
    data = json.loads(json.loads(message.value())["content"])[0]
    if data["T"] == "n":
        url = data["url"]
        downloaded = fetch_url(url)
        text = extract(downloaded)
         
        result = {}
        result["headline"] = data["headline"]
        result["created_at"] = data["created_at"]
        result["updated_at"] = data["updated_at"]
        result["symbols"] = data["symbols"]
        result["content"] = text
        producer.send("processedNews", result)

with DAG(
    dag_id = "kafka_raw_producer_links",
    schedule = "@daily",
    start_date = datetime(2023, 9, 7),
    catchup = False
):
    consume_raw_news = ConsumeFromTopicOperator(
        task_id="consume_raw_news",
        kafka_config_id="kafka",
        topics=["bronzeNews"],
        apply_function=webScrape,
        poll_timeout=60,
        max_batch_size=20,
        # outlets=[news_links],
    )