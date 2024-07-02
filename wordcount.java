
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

//import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;



//@SuppressWarnings("deprecation")
public class Wordcount {
	
	private final static String INPUT_FILE_TEXT = "/Users/B0218993/spark3/abc.txt";
	private final static String OUTPUT_FILE_TEXT = "/Users/B0218993/spark3/out2";
	
	//@SuppressWarnings({ "unchecked", "deprecation" })
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count with Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile(INPUT_FILE_TEXT);
		System.out.println(lines);
		final Broadcast<List<String>> wordsToIgnore = sc.broadcast(getWordsToIgnore());
		
		
		lines = lines.flatMap(line -> Arrays.asList(line.split(".")).iterator());
		
		/**Pair the word with count*/
		JavaPairRDD<String, Integer> mapToPair = lines.filter(word -> !wordsToIgnore.value().contains(word)).mapToPair(w -> new Tuple2<>(w, 1));
		/**Reduce the pair with key and add count*/
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((wc1, wc2) -> wc1 + wc2);
		System.out.println(reduceByKey.collectAsMap());
		
		//reduceByKey.saveAsTextFile(OUTPUT_FILE_TEXT);

		
		sc.close();
		
	}
	private static List<String> getWordsToIgnore() {
		return Arrays.asList("the", "of", "and", "for");
	}


}
