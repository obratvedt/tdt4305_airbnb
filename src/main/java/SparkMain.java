
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.scalatest.EncodedOrdering;
import scala.Double;
import scala.Tuple2;
import scala.Tuple3;
import schemas.ListingsSchema;
import schemas.ReviewSchema;
import tasks.Task3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkMain {

    private static Dataset<Row> getReviewDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .schema(ReviewSchema.getReviewSchema())
                .csv("airbnb_datasets/reviews_us.csv");
    }

    private static Dataset<Row> getListingDs(SparkSession sparkSession) {
        return sparkSession
                .read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferschema", true)
                //.schema(ListingsSchema.getListingsSchema())
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

        Dataset<Row> reviewsDs = getReviewDs(sparkSession);
        Dataset<Row> listingDs = getListingDs(sparkSession);
        

    }

}

