import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMain {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb analyis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> ds = sparkSession
                .read()
                .option("header", true)
                .csv("airbnb_datasets/reviews_us.csv");
        ds.show();

    }

}

