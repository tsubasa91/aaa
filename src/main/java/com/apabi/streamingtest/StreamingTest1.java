package com.apabi.streamingtest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;

public class StreamingTest1 {


    public static void main(String[] args) {

        //获取sparkconfig
        SparkConf sparkConf =new SparkConf().setAppName("streaming01sssssssss").setMaster("local[2]");
        sparkConf.set("spark.driver.allowMultipleContexts","true");
//        //获取sparkcontext
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //设置日志级别
//        sparkContext.setLogLevel("WARN");
        //构建streamingcontext
        System.setProperty("hadoop.home.dir", "F:\\apache-flume-1.8.0-bin\\conf");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("172.18.89.206", 9999);

        JavaDStream<String> objectDStream = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");
                return (Iterator<String>) Arrays.asList(split);
            }
        });

        final JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = objectDStream.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });



    }

}
