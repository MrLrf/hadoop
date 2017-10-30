package com.lirf.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;


public class WordCount {
    public static void main(String[] args) {

        //创建 SparkConf对象，对程序进行必要的配置
        SparkConf conf = new SparkConf()
                .setAppName("WordCount").setMaster("local");

        //通过conf创建上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> lines = sc.textFile("D://spark.txt");

        //----用各种Transformation算子对RDD进行操作-----------------------------------------
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(String line) throws Exception {
                // TODO Auto-generated method stub
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                // TODO Auto-generated method stub
                return v1 + v2;
            }
        });


        //----用一个 action 算子触发job-----------------------------------------
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times");
            }
        });
    }
}