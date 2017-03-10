import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import schemas.CalendarSchema;
import schemas.ListingsSchema;
import schemas.ReviewSchema;

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


        Dataset<Row> ds = getListingDs(sparkSession);

        ds.select("price")
                .map( row -> Float.valueOf(
                            ((String) row.getAs("price"))
                            .replace("$", "")
                            .replace(",", ""))

                , Encoders.FLOAT() )
                .filter( functions.col("value").gt(0) )
                .agg( functions.avg("value") )
                .show();

        //System.out.println(val);

        //ds.show();
        /*

        Dataset<Row> priceAvg = ds
                .filter( functions.col("price").notEqual(0) )
                .agg( functions.avg("price") );

        priceAvg.show();
*/
    }

}

