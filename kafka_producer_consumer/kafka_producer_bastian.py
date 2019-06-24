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
import random
import time

def main():
    #Load the whole file to a map
    f = open("/home/xiaoling/Insight_DE/data/X_test.csv", "r")
    index2line={}
    
    index=0    
    header= False
    while True:
        line = f.readline().rstrip('\n')
        if not line:
            break
            
        if header:
            header= False
        else: 
            index2line[index]= line  #0-> ; 1-> 
        index= index+ 1
              
    # key, index, phone number, msg, time () 
    
    #send message to the kafka
    time_length= 10
    time_start= time.time()
    frequency=1
    producer = KafkaProducer(bootstrap_servers = 'ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092')
    
    key=0 #The serial key of the current message 
    index_max= len(index2line)-1
    print(index_max)
    while time.time()- time_start < time_length:   
        index= random.randint(0, index_max)    
        phone_number="("+ str(random.randint(100,999) )+")-"+ str( random.randint(100, 999) )+ "-"+ str( random.randint(1000, 9999) )     
        line= index2line[index] 
        time_produced= str( time.time() )
        msg= str(key)+","+ str(index)+ ","+ phone_number+"," +line+ ","+ time_produced
        producer.send('transactions_forward', msg)
        producer.flush()
        print msg
        print "\n"
        time.sleep(1.0/ frequency)
        
        key+= 1

    return

if __name__ == '__main__':
    main()
