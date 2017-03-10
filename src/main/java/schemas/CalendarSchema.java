package schemas;


import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CalendarSchema {

    public static StructType getCalendarSchema() {
        StructType calendarSchema = DataTypes
                .createStructType(new StructField[]{
                        DataTypes.createStructField("listing_id", DataTypes.IntegerType, true),
                        DataTypes.createStructField("date", DataTypes.DateType, true),
                        DataTypes.createStructField("available", DataTypes.StringType, true)
                });

        return calendarSchema;
    }
}
