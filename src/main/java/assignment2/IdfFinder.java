package assignment2;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.spark.InternalAccumulator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import scala.Tuple2;
import scala.reflect.ClassTag;

import javax.xml.crypto.Data;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sigurd on 3/28/17.
 */
public class IdfFinder {

    /**
     *
     * WARINING: Only feed me with lowercase and no punctuations
     * @param descriptions, listings with ID, and descriptions, cleaned beforehand
     * @param words words which frequencies need to be calculated
     * @return inverseDocumentFrequency
     */
    public static void inverseDocumentFrequency(Dataset<String> descriptions, String words){
        long documentCount = descriptions.count();
        List<String> wordList = Arrays.asList(words.split(" "));

        descriptions
                .toJavaRDD()
                .map(s -> new HashSet<String>(Arrays.asList(s.split(" "))).toString().replace(",", "").replace("]", "").replace("[", ""))
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(s -> wordList.contains(s))
                .mapToPair(s -> new Tuple2<String, Integer>(s,1))
                .reduceByKey((a,b) -> a+b)
                .map(tuple -> new Tuple2<String, Float>(tuple._1,  (float) documentCount/ tuple._2))
                .coalesce(1)
                .saveAsTextFile("test");


    }

}
