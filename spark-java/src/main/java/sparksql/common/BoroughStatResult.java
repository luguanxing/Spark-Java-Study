package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

@Data
@AllArgsConstructor
public class BoroughStatResult {
    String license;
    String borough;
    Long costTime;
}
