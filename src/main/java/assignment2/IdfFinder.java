package assignment2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.util.Set;

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
    public Dataset<Tuple2<String, Long>> inverseDocumentFrequency(Dataset<Row> descriptions, Dataset<String> words){
        long documentCount = descriptions.count();

        return words.map( word -> {

            long documentFrequency = descriptions.map( listing -> {
                String description = listing.getAs("description");
                return description.contains(word);
            }, Encoders.BOOLEAN()).count();

            return new Tuple2<>(word, documentFrequency/documentCount);
        }, Encoders.tuple(Encoders.STRING(), Encoders.LONG()) );

    }

}
