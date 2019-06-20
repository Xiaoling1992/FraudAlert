############################################################
# This python script is a producer for kafka.

# To send it to kafka, each record is first converted to
# string then to bytes using str.encode('utf-8') method.
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of the servers
# were written in a separate "config.py".
############################################################


from kafka import KafkaProducer
import time

def main():

    producer = KafkaProducer(bootstrap_servers = 'ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092')

    f = open('test.txt', 'r')
    while True:
        msg = f.readline()
        if not msg:
            break
        producer.send('test-topic', msg)
        producer.flush()
        print msg
        time.sleep(1)
    f.close()


    return

if __name__ == '__main__':
    main()
