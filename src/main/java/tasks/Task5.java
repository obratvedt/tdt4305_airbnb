package tasks;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class Task5 {
    
    public static void topGuests(Dataset<Row> listingDs, Dataset<Row> reviewDs) {
        Dataset<Row> joinedSet = reviewDs
                .select("reviewer_id", "listing_id", "reviewer_name")
                .join(listingDs.select("id", "city")
                        , listingDs.col("id").equalTo(reviewDs.col("listing_id")));

        Dataset<Row> test = joinedSet.select("reviewer_id", "city", "reviewer_name")
                .groupBy("city", "reviewer_id", "reviewer_name")
                .count();

        WindowSpec w = Window.partitionBy(test.col("city")).orderBy(test.col("count").desc());

        test.withColumn("rank", row_number().over(w)).where(col("rank").$less$eq(3)).show();

    }
}
