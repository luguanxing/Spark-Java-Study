package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;
import java.util.Locale;

@Data
@AllArgsConstructor
public class Trip {
    String license;
    Long pickUpTs;
    Long dropOffTs;
    Double pickUpY;
    Double pickUpX;
    Double dropOffY;
    Double dropOffX;

    public static Trip parseRowToTrip(Row row) {
        try {
            // 解析数据行
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss", Locale.ENGLISH);
            String license = row.getAs("hack_license").toString();
            Long pickupTs = sdf.parse(row.getAs("pickup_datetime").toString()).getTime();
            Long dropoffTs = sdf.parse(row.getAs("dropoff_datetime").toString()).getTime();
            Double pickupY = Double.parseDouble(row.getAs("pickup_longitude").toString());
            Double pickupX = Double.parseDouble(row.getAs("pickup_latitude").toString());
            Double dropoffY = Double.parseDouble(row.getAs("dropoff_longitude").toString());
            Double dropoffX = Double.parseDouble(row.getAs("dropoff_latitude").toString());
            return new Trip(license, pickupTs, dropoffTs, pickupY, pickupX, dropoffY, dropoffX);
        } catch (Exception e) {
            // 忽略异常的数据行
            System.err.println("异常数据：" + row.toString());
            return null;
        }
    }
}
