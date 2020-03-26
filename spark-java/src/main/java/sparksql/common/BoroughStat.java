package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

@Data
@AllArgsConstructor
public class BoroughStat {
    String license;
    String pickUpBorough;
    String dropBorough;
    Long pickUpTs;
    Long dropOffTs;

    public static BoroughStat parseRowToBoroughStat(Row row) {
        try {
            // 解析数据行
            String license = row.getAs("license").toString();
            String pickUpBorough = row.getAs("pickUpBorough").toString();
            String dropBorough = row.getAs("dropBorough").toString();
            Long pickUpTs = Long.parseLong(row.getAs("pickUpTs").toString());
            Long dropOffTs = Long.parseLong(row.getAs("dropOffTs").toString());
            return new BoroughStat(license, pickUpBorough, dropBorough, pickUpTs, dropOffTs);
        } catch (Exception e) {
            // 忽略异常的数据行
            System.err.println("异常数据：" + row.toString());
            return null;
        }
    }
}
