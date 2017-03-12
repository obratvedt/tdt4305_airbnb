
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.scalatest.EncodedOrdering;
import scala.Tuple2;
import schemas.ListingsSchema;
import schemas.ReviewSchema;

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


    /* Task 1.3 - 2 b) */
    private static void distinctListings(Dataset<Row> listingsDs) {
        List<String> results = new ArrayList<>();
        Arrays.stream(listingsDs.schema().fieldNames())
                .forEach( (fieldName) -> {
                    long count = listingsDs.select( fieldName ).distinct().count();
                    results.add(fieldName + ":" + count);
                });
        results.forEach( result -> System.out.println(result));
    }


    /* Task 1.3 - 2 c) */
    private static void listCities(Dataset<Row> listingsDs){
        List<String> cities = new ArrayList<>();
        listingsDs
                .select("city")
                .distinct()
                .collect();
    }

    /* Task 1.3 - 3 a) */
    private static void avgCityPrice(Dataset<Row> listingsDs){
        listingsDs.select("city", "price")
                .map( row -> {
                    String priceString = row.getString(1);
                    float price = Float.valueOf(priceString
                            .replace("$","")
                            .replace(",",""));
                    return new Tuple2<>(row.getString(0) ,price);
                }, Encoders.tuple(Encoders.STRING(), Encoders.FLOAT()))
                .toDF("city", "price")
                .groupBy("city")
                .agg(functions.avg("price").as("avgprice"))
                .orderBy(functions.col("avgprice").desc())
                .coalesce(1)
                .write()
                .csv("./output/avgCityPrice");
    }

    /* Task 1.3 - 3 b) */
    private static void avgRoomTypePrice(Dataset<Row> listingsDs){
        listingsDs.select("room_type", "price")
                .map( row -> {
                    String priceString = row.getString(1);
                    float price = Float.valueOf(priceString
                            .replace("$","")
                            .replace(",",""));
                    return new Tuple2<>(row.getString(0) ,price);
                }, Encoders.tuple(Encoders.STRING(), Encoders.FLOAT()))
                .toDF("room_type", "price")
                .groupBy("room_type")
                .agg(functions.avg("price").as("avgprice"))
                .orderBy(functions.col("avgprice").desc())
                .coalesce(1)
                .write()
                .csv("./output/avgRoomTypePrice");
    }

    /* Task 1.3 - 3 c) */
    private static void avgNumReviewPerMonth(Dataset<Row> reviewsDs){
        reviewsDs
                .select("id", "date")
                .map( row -> {
                    String month = row.getString(1)
                            .substring(7);
                    return new Tuple2<Integer, String>(row.getInt(0), month);
                }, Encoders.tuple(Encoders.INT(), Encoders.STRING()))
                .groupBy("date")
                .count()
                .coalesce(1)
                .write()
                .csv("./output/avgNumReviewPerMonth");
    }


    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Airbnb analyis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> reviewsDs = getReviewDs(sparkSession);
        avgNumReviewPerMonth(reviewsDs);
        reviewsDs.show();

        //distinctListings(listingDs);
        /*
        Dataset<Row> listingDs = getListingDs(sparkSession);
        avgCityPrice(listingDs);
        avgRoomTypePrice(listingDs);*/

        /*
        ds.select("price")
                .map( row -> Float.valueOf(
                            ((String) row.getAs("price"))
                            .replace("$", "")
                            .replace(",", ""))

                , Encoders.FLOAT() )
                .filter( functions.col("value").gt(0) )
                .agg( functions.avg("value") )
                .show();*/

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

