package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Product {
    String name;
    String category;
    Integer price;
}
