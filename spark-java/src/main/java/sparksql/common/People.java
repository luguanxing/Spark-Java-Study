package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class People {
    Integer id;
    String name;
    Integer cityId;
}
