############################################################
# This python script is a producer for kafka.

# To send it to kafka, each record is first converted to
# string then to bytes using str.encode('utf-8') method.
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of the servers
# were written in a separate "config.py".
############################################################


from kafka import KafkaConsumer
import time

def main():
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer("test-topic", group_id="test", bootstrap_servers = 'ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092' )
    
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    for message in consumer:
        print  "%s:%d:%d:  value=%s" % (message.topic, message.partition, message.offset, message.value)
    
    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)
    
    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=5000)
    return


if __name__ == '__main__':
    main()
