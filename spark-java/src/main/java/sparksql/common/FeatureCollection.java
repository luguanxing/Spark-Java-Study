package sparksql.common;


import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class FeatureCollection implements Serializable {
    List<Feature> features;
}
