package assignment2;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory;
import org.apache.spark.InternalAccumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.codehaus.janino.Java;
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
    public static Dataset<Row> inverseDocumentFrequency(Dataset<String> descriptions, Dataset<String> words){
        long documentCount = descriptions.count();
        List<String> wordList = Arrays.asList(words.first().split(" "));

        return descriptions
                .map(s -> new HashSet<String>(Arrays.asList(s.split(" "))).toString()
                        .replace(",", "")
                        .replace("]", "")
                        .replace("[", "")
                , Encoders.STRING())
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING())
                .filter(s -> wordList.contains(s))
                .groupBy(functions.col("value"))
                .agg(functions.count("value").as("count"))
                .map( row -> {

                    String word = row.getAs("value");
                    float idf = (float) documentCount / ((long) row.getAs("count"));
                    return RowFactory.create( word, idf );
                },
                    RowEncoder.apply(
                            DataTypes.createStructType( new StructField[]{
                                            DataTypes.createStructField("word", DataTypes.StringType, true ),
                                            DataTypes.createStructField("idf", DataTypes.FloatType, true ),
                                    })
                    )
                );
    }

}
