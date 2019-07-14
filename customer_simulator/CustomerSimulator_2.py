############################################################
# This python script is a consumer fo Kafka;

#This script is also a user simulator.If a transaction is predicted to be fraud, it will be sent to a simulator and wait for the 
#reply of the cardholder there. 

#I will assign a waiting time for each transaction. If the waiting time is less than 20 s (for example), the system will accept or 
#reject the transaction based on the reply automatically. 

#If the waiting time is larger than 20 s, the transaction will be marked as 'nonreply' and sent to the banker, who will decide to 
#accept or reject the transaction manually. (For example, banker will give a call to the cardholder. If it is not a fraud, the banker 
#should accept the transaction otherwise reject

#Even though the message is responded after the max_waiting_time, the reply will not be recorded for simplicity.
############################################################


from kafka import KafkaConsumer
import time
import psycopg2
import numpy as np

def main():
    #Load the Customer_2fraud map
    f = open("/home/ubuntu/data/index_2fraud.dat", "r")
    index2fraud={}
    header= False
    while True:
        line = f.readline().rstrip('\n');
        if not line:
            break            
        if header:
            header= False
        else: 
            line=line.split(",")
            index= int(line[0])
            fraud= int(line[1])
            index2fraud[index]= fraud  #0-> ; 1-> 
    print ("how many indexs: "+ str(len(index2fraud)) )
   
   # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer("transactions-backward", group_id="CustomerSimulator", bootstrap_servers = 'ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092' )
    
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    index_index, index_time_produced, index_time_processed, index_latency, index_reply=1, 33, 34, 35, 37
    connection = psycopg2.connect(user="db_select",
                                  password="password",
                                  host="10.0.0.4",
                                  port="5431",
                                  database="test_db")
    cursor = connection.cursor()
    print ( connection.get_dsn_parameters(),"\n")
    postgres_insert_query = """INSERT INTO transactions (key,index,phonenumber,time,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,v21,v22,v23,v24,v25,v26,v27,v28,amount,timeproduced,timeprocessed,latency,prediction,reply) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    max_waiting_time= 5000 #10000 ms, 10s
    for message in consumer:
        transaction=message.value.split(",")
        #print(len(transaction ) ) #Should be 38
        #update the time_processed, latency and reply
        index= int( transaction[index_index])
        time_produced= long( transaction[index_time_produced] )
        #time_processed= long( transaction[index_time_processed] )
        
        time_now= long(time.time()*1000 )
        waiting_time= np.random.poisson(5000)        
        if waiting_time<= max_waiting_time:
            time_processed= time_now + waiting_time
            latency= time_processed- time_produced
        
            transaction[ index_time_processed ]= str( time_processed );
            transaction[ index_latency]= str(latency)
            transaction[ index_reply]= "yes" if index2fraud[index]==1 else "no"
            #print(index, time_produced, time_processed, latency, transaction[index_reply], waiting_time)
        else:
            time_processed= time_now + max_waiting_time
            latency= time_processed- time_produced        
            transaction[ index_time_processed ]= str( time_processed );
            transaction[ index_latency]= str(latency)
            transaction[ index_reply]= "noreply"
            #print(index, time_produced, time_processed, latency, transaction[index_reply], waiting_time)

            
        try:      
            record_to_insert = (transaction[0],transaction[1],transaction[2],transaction[3],transaction[4],transaction[5],transaction[6],transaction[7],transaction[8],transaction[9],transaction[10],transaction[11],transaction[12],transaction[13],transaction[14],transaction[15],transaction[16],transaction[17],transaction[18],transaction[19],transaction[20],transaction[21],transaction[22],transaction[23],transaction[24],transaction[25],transaction[26],transaction[27],transaction[28],transaction[29],transaction[30],transaction[31],transaction[32],transaction[33],transaction[34],transaction[35], transaction[36],transaction[37])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            count = cursor.rowcount
            print (count, "Record inserted successfully into mobile table")        
       
        except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to insert record into transactions table", error)
       
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
    
    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=100000)
    


if __name__ == '__main__':
    main()
