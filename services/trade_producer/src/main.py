from quixstreams import Application
from time import sleep

def produce_trades(
        kafka_broker_address: str,
        kafka_topic_name: str,
) -> None:
    """
    This function is responsible for producing trade data to a Kafka topic.

    Parameters:
    kafka_broker_address (str): The address of the Kafka broker.
    kafka_topic (str): The name of the Kafka topic to which the trade data will be produced.

    Returns:
    None: This function does not return any value.

    """
    app = Application(broker_address = kafka_broker_address)

    # Define a topic "my_topic" with JSON serialization
    topic = app.topic(name = kafka_topic_name, value_serializer = 'json')

    event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}


    # Create a Producer instance
    with app.get_producer() as producer:

        while True:
            # Serialize an event using the defined Topic
            message = topic.serialize(key=event["id"], value=event)

            # Produce a message into the Kafka topic
            producer.produce(
                topic=topic.name, 
                value=message.value, 
                key=message.key
            )
            print("Produced message to Kafka topic")

            sleep(2)


if __name__ == "__main__":
    produce_trades(
        kafka_broker_address = "localhost:19092",
        kafka_topic_name = "trade"
    )


    
