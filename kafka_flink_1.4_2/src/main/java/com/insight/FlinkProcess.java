package com.insight;


import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;


//import com.insight.HostURLs.*;


public class FlinkProcess {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure event-time characteristics
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // generate a Watermark every second
        env.getConfig().setAutoWatermarkInterval(1000);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start kafka producer -> now use python producer 'KProducer'
        //KProducer kp = new KProducer();
        //kp.kafkaProducer();

        // call HostURLs class to get URLs for all services
        //HostURLs urls = new HostURLs();


        // create new property of Flink
        // set Zookeeper and Kafka url
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "ec2-34-218-113-11.us-west-2.compute.amazonaws.com:2181,ec2-35-161-124-47.us-west-2.compute.amazonaws.com:2181,ec2-52-34-133-46.us-west-2.compute.amazonaws.com:2181");
        properties.setProperty("bootstrap.servers", "ec2-34-218-113-11.us-west-2.compute.amazonaws.com:9092,ec2-35-161-124-47.us-west-2.compute.amazonaws.com:9092,ec2-52-34-133-46.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("group.id", "myGroup");                 // Consumer group ID
        properties.setProperty("auto.offset.reset", "earliest");       // Always read topic from start
        //properties.setProperty("group.id", "flink_consumer");


        // read 'topic' from Kafka producer: Kafka topic is "wikiInput"
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("test-topic",
                new SimpleStringSchema(), properties);

        // convert kafka stream to data stream
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer);

        rawInputStream.print();

/*

        // input data: ['revision', 'article_id', 'rev_id', 'article_title', 'event_time', 'username', 'userid', 'proc_date', 'proc_time']
        // output data: ['partition id', 'proc_time', 'count']
        // transformation: count all input within 0.5s time window
        // save output to Cassandra table: allInputCountSecond
        // table:
        DataStream<Tuple3<String, String, Long>> windowedoutput = rawInputStream
                .map(line -> line.split(" "))
                .flatMap(new FlatMapFunction<String[], Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String[] s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        collector.collect(new Tuple3<String, String, Long>("a", s[7] + " " + s[8], 1L));

                    }
                })
                .keyBy(0, 1)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
                .sum(2);


        CassandraSink.addSink(windowedoutput)
                .setQuery("INSERT INTO ks.totalInputCountSecond (global_id, proc_time, count) " +
                        "values (?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();


        // input data: ['revision', 'article_id', 'rev_id', 'article_title', 'event_time', 'username', 'userid', 'proc_date', 'proc_time']
        // output data: ['username', 'proc_time', 'count']
        // transformation: count all input within 0.5s time window
        // save output to Cassandra table: singleUserCountSecond
        // table:
        DataStream<Tuple3<String, String, Long>> singleUserCount = rawInputStream
                .map(line -> line.split(" "))
                .flatMap(new FlatMapFunction<String[], Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String[] s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        collector.collect(new Tuple3<String, String, Long>(s[5], s[7] + " " + s[8], 1L));

                    }
                })
                .keyBy(0, 1)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .sum(2);


        CassandraSink.addSink(singleUserCount)
                .setQuery("INSERT INTO ks.singleUserCount (username, proc_time, count) " +
                        "values (?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();


        // input data: ['revision', 'article_id', 'rev_id', 'article_title', 'event_time', 'username', 'userid', 'proc_date', 'proc_time']
        // output data: ['global_id', 'proc_time', 'username', 'count']
        // transformation: count all input within 0.5s time window
        // save output to Cassandra table: flaggedUser
        // table:
//        DataStream<Tuple4<String, String, String, Long>> flaggedUser = rawInputStream
//                .map(line -> line.split(" "))
//                .flatMap(new FlatMapFunction<String[], Tuple4<String, String, String, Long>>() {
//                    @Override
//                    public void flatMap(String[] s, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
//                        collector.collect(new Tuple4<String,String, String, Long>("a", s[7]+" "+s[8], s[5], 1L));
//
//                    }
//                })
//                .keyBy(0, 1, 2)
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
//                .sum(3)
//                .filter(a -> a.f3 > 50);



        DataStream<Tuple4<String, String, String, Long>> flaggedUser = singleUserCount
                .map(new MapFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>>() {
                    @Override
                    public Tuple4<String, String, String, Long> map(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
                        return new Tuple4<String, String, String, Long>("a", stringStringLongTuple3.f1, stringStringLongTuple3.f0, stringStringLongTuple3.f2);
                    }
                })
                .filter(a -> a.f3 > 100);


        CassandraSink.addSink(flaggedUser)
                .setQuery("INSERT INTO ks.flaggedUser (global_id, proc_time, username, count) " +
                        "values (?,?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();


//        // count the total number of processed line in window of 5 sec
//        DataStream<Tuple2<Long, Long>> winOutput = rawInputStream
//                .map(line -> line.split(" "))
//                .flatMap(new FlatMapFunction<String[], Tuple2<Long, Long>>() {
//
//                    // Create an accumulator
//                    private long linesNum = 0;
//
//
//                    @Override
//                    public void flatMap(String[] lines, Collector<Tuple2<Long, Long>> out) throws Exception {
//                        out.collect(new Tuple2<Long, Long>(linesNum++, 1L));
//
//                    }
//                })
//                .assignTimestampsAndWatermarks(new RegionInfoTimeStampGenerator())
//                .keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(50)))
//                .reduce((a,b) -> new Tuple2<>(a.f0, a.f1+b.f1));


//        // Tuple2 save two elements pair
//        DataStream<Tuple2<Long, String>> result =
//                // split up the lines in pairs (2-tuples) containing: (word,1)
//                rawInputStream.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
//                    //@Override
//                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
//                        // normalize and split the line
//                        String[] words = value.toLowerCase().split("\\W+");
//
//                        // emit the pairs
//                        for (String word : words) {
//                            //Do not accept empty word, since word is defined as primary key in C* table
//                            if (!word.isEmpty()) {
//                                out.collect(new Tuple2<Long, String>(1L, word));
//                            }
//                        }
//                    }
//                });



*/


        env.execute("Kafka to Flink Streaming");

    }

/*    private static class SumAggregation implements AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {

        // Similar to WinRateAggregator, but also count the number of occurences.
        // Input: <hero_id:String,win/lose:Integer,start_time:Long>
        // Accumulator: <hero_id:String,total_win:Long,total_played:Long,latest_start_time:Long>
        // Output: <hero_id:String,win_rate:Float,count:Long,latest_start_time:Long>

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return new Tuple2<>("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> value, Tuple2<String, Long> accumulator) {
            // Update accumulator with the new value. Here we update the start_time to be the latest
            return new Tuple2<>(value.f0,
                    accumulator.f1 + value.f1);

        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
            return null;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
            return null;
        }


    }*/

}
