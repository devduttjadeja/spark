/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaWordCount {
	
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    //JavaSparkContext sc = new JavaSparkContext(conf);
    
    // Dataset<Row> textFile = spark.read().csv("src\\main\\java\\spark\\http-flood.csv");
    // JavaRDD<Row> javaRDD = textFile.select("_c2").javaRDD();
    // RDD<Row> rdd = textFile.select("_c2").rdd();
    // javaRDD.
    		
    /*
    val file = spark.read.csv("C:/Users/Admin/Desktop/http-flood.csv")
	file.show
	val df = file.select("_c2").rdd
	df.collect
	val temp = df.map(word=>(word,1))
	temp.collect
	val output = temp.reduceByKey(_ + _)  
    */
    
    JavaRDD<String> lines = spark.read().textFile("src\\main\\java\\spark\\http-flood.csv").javaRDD();
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    List<Tuple2<String, Integer>> output = counts.collect();
    
    File file = new File("logs1.txt");
    FileWriter writer = new FileWriter(file);
    
    for (Tuple2<?,?> tuple : output) {
      // System.out.println(tuple._1() + ": " + tuple._2());
    	writer.write(tuple._1() + ": " + tuple._2()+"\n");
    }
    writer.close();
    spark.stop();
  }
}
