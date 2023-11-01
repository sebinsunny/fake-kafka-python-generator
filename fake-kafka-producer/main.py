from faker import Faker
import json
from confluent_kafka import Producer
import argparse
from pizzaproducer import PizzaProvider
from userbehaviorproducer import UserBehaviorProvider
from stockproducer import StockProvider
from realstockproducer import RealStockProvider
from metricproducer import MetricProvider
from userbets import UserBetsProvider
from rolling import RollingProvider
from metricadvancedproducer import MetricAdvancedProvider
from loadproducer import LoadProvider


MAX_NUMBER_PIZZAS_IN_ORDER = 10
MAX_ADDITIONAL_TOPPINGS_IN_PIZZA = 5


# Creating a Faker instance and seeding to have the
# same results every time we execute the script
fake = Faker()
Faker.seed(4321)


# function produce_msgs starts producing messages with Faker
def produce_msgs(
    security_protocol="SSL",
    sasl_mechanism="SCRAM-SHA-256",
    cert_folder="~/kafka-pizza/",
    username="",
    password="",
    hostname="hostname",
    port="1234",
    topic_name="pizza-orders",
    nr_messages=-1,
    max_waiting_time_in_sec=5,
    subject="pizza",
    compression_type=None,
):
    print("Using compression_type: ", compression_type)

    if nr_messages <= 0:
        nr_messages = float("inf")
    i = 0

    if subject == "stock":
        fake.add_provider(StockProvider)
    elif subject == "realstock":
        fake.add_provider(RealStockProvider)
    elif subject == "metric":
        fake.add_provider(MetricProvider)
    elif subject == "advancedmetric":
        fake.add_provider(MetricAdvancedProvider)
    elif subject == "userbehaviour":
        fake.add_provider(UserBehaviorProvider)
    elif subject == "bet":
        fake.add_provider(UserBetsProvider)
    elif subject == "rolling":
        fake.add_provider(RollingProvider)
    elif subject == "load":
        fake.add_provider(LoadProvider)
    else:
        fake.add_provider(PizzaProvider)
    print("SUBJECT: ", subject)
    conf = {'bootstrap.servers': hostname + ":" + port,
            'security.protocol': 'SSL',
            'ssl.ca.location': cert_folder + "/ca.pem",
            'ssl.key.location': cert_folder + "/service.key",
            'ssl.certificate.location': cert_folder + "/service.cert",
            'client.id': 'ThirdpartyIngestion',
            'batch.size': 50000,  # Allow up to 50,000 messages in a batch
            'linger.ms': 100,
            }
    if compression_type:
        conf['compression.type'] = compression_type
    producer = Producer(conf)
    while i < nr_messages:
        if subject in [
            "stock",
            "userbehaviour",
            "realstock",
            "metric",
            "bet",
            "rolling",
            "advancedmetric",
            "load",
        ]:
            message, key = fake.produce_msg()

            def delivery_report(err, msg):
                if err is not None:
                    print('Message delivery failed: {}'.format(err))
                else:
                    print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

            print("Sending: {}".format(message))
            serialized_key = json.dumps(key).encode("ascii")
            serialized_message = json.dumps(message).encode("ascii")
            producer.poll(0)
            producer.produce(topic_name, key=serialized_key, value=serialized_message, callback=delivery_report)
            # Force flushing of all omessages
            if (i % 1000) == 0:
                producer.flush()
            i = i + 1





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--security-protocol",
        help="""Security protocol for Kafka
                (PLAINTEXT, SSL, SASL_SSL)""",
        required=True,
    )
    parser.add_argument(
        "--sasl-mechanism",
        help="""SASL mechanism for Kafka
                (PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512)""",
        required=False,
    )
    parser.add_argument(
        "--cert-folder",
        help="""Path to folder containing required Kafka certificates.
                Required --security-protocol equal SSL or SASL_SSL""",
        required=False,
    )
    parser.add_argument(
        "--username",
        help="Username. Required if security-protocol is SASL_SSL",
        required=False,
    )
    parser.add_argument(
        "--password",
        help="Password. Required if security-protocol is SASL_SSL",
        required=False,
    )
    parser.add_argument(
        "--host",
        help="Kafka Host (obtained from Aiven console)",
        required=True,
    )
    parser.add_argument(
        "--port",
        help="Kafka Port (obtained from Aiven console)",
        required=True,
    )
    parser.add_argument("--topic-name", help="Topic Name", required=True)
    parser.add_argument(
        "--nr-messages",
        help="Number of messages to produce (0 for unlimited)",
        required=True,
    )
    parser.add_argument(
        "--max-waiting-time",
        help="Max waiting time between messages (0 for none)",
        required=True,
    )
    parser.add_argument(
        "--subject",
        help="""What type of content to produce (possible choices areL
                [pizza, userbehaviour, stock,
                realstock, metric, advancedmetric]
                pizza is the default""",
        required=False,
    )
    parser.add_argument(
        "--compression-type",
        help=""
    )
    args = parser.parse_args()
    p_security_protocol = args.security_protocol
    p_cert_folder = args.cert_folder
    p_username = args.username
    p_password = args.password
    p_sasl_mechanism = args.sasl_mechanism
    p_hostname = args.host
    p_port = args.port
    p_topic_name = args.topic_name
    p_subject = args.subject
    p_compression_type = args.compression_type
    produce_msgs(
        security_protocol=p_security_protocol,
        cert_folder=p_cert_folder,
        username=p_username,
        password=p_password,
        hostname=p_hostname,
        port=p_port,
        topic_name=p_topic_name,
        nr_messages=int(args.nr_messages),
        max_waiting_time_in_sec=float(args.max_waiting_time),
        subject=p_subject,
        compression_type=p_compression_type,
        sasl_mechanism=p_sasl_mechanism,
    )
    print(args.nr_messages)


if __name__ == "__main__":
    main()
