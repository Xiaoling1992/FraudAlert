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
    """Load the whole file to a map, method's description"""
    # customer_id, phone number, featueres.
    f = open("/home/ubuntu/data/X_test_2.csv", "r")
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

   
    producer = KafkaProducer(bootstrap_servers = 'ec2-34-218-113-11.us-west-2.compute.amazonaws.com:9092,ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092,ec2-52-34-133-46.us-west-2.compute.amazonaws.com:9092')
    index_max= len(index2line)-1
    print(index_max)
    
    key=0 #The serial key of the current message 
    index= random.randint(0, index_max)       
    while key< 600000:   
           
        #phone_number="("+ str(random.randint(100,999) )+")-"+ str( random.randint(100, 999) )+ "-"+ str( random.randint(1000, 9999) )     
        line= index2line[index] 
        time_produced= long( 1000* time.time() );
        msg= str(key)+"," +line+ ","+ str(time_produced)
        #msg= str(key)+","+ str(index)+ ","+ phone_number+"," +line+ ","+ str(time_produced)
        producer.send('transactions-forward', msg)
        producer.flush()
        #print msg+ "\n"
        #time.sleep(1.0/ frequency)
        
        key+= 1
        index= index+ 1 if index< index_max else 0  #[0, index_max]

if __name__ == '__main__':
    main()
