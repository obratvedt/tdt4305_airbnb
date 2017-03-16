
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.scalatest.EncodedOrdering;
import scala.Double;
import scala.Tuple2;
import scala.Tuple3;
import tasks.Task2;
import tasks.Task3;
import tasks.Task4;
import tasks.Task5;


public class SparkMain {

    private static Dataset<Row> getReviewDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .csv("airbnb_datasets/reviews_us.csv");
    }

    private static Dataset<Row> getListingDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/listings_us.csv");
    }

    private static Dataset<Row> getCalendarDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                .csv("airbnb_datasets/calendar_us.csv");

    }

    //Not working yet, need a parser.
    private static Dataset<Row> getNeighbourhoodsDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .json("airbnb_datasets/neighbourhoods.geojson");
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb analyis")
                .master("local[*]")
                .getOrCreate();

        Task5.richestGuest(getReviewDs(sparkSession), getListingDs(sparkSession));

    }

}

