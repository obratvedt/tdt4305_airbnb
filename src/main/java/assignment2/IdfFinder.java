package assignment2;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.reflect.ClassTag;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
    public Dataset<Tuple2<String, Long>> inverseDocumentFrequency(Dataset<String> descriptions, String words){
        long documentCount = descriptions.count();


        String wordCounts = descriptions
                .map( description -> {
                    final String cleanDescription = " " + description + " ";

                    List<String> matches = Arrays.stream(words.split(" ")).map(word -> {
                        return cleanDescription.contains(" " + word + " ") ? "t": "f";
                    }).collect(Collectors.toList());

                    String matchesList = String.join(",", matches);
                    return matchesList;
                    //return Tuple2<String, String>("matches", matches);
                }, Encoders.STRING() )
                //}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()) )
                .reduce( (listingMatches, accumulator) -> {
                    String[] accumulated = accumulator.split(",");
                    String[] matches = listingMatches.split(",");
                    for(int i = 0; i> matches.length; i++){
                        if(matches[i].equals("t")){
                            int currentValue = Integer.parseInt(accumulated[i]);
                            accumulated[i] = currentValue + 1 + "";
                        }
                    }

                    return String.join(",", accumulated);
                });

        System.out.println(wordCounts);

        return null;
    }

}
