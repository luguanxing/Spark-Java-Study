package sparksql.common;


import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.google.gson.Gson;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Feature implements Serializable {
    Integer id;
    Map<String, Object> properties;
    Map<String, Object> geometry;

    public Geometry getGeometry() {
        MapGeometry mapGeometry = GeometryEngine.geoJsonToGeometry(new Gson().toJson(geometry), 0, Geometry.Type.Unknown);
        Geometry geometry = mapGeometry.getGeometry();
        return geometry;
    }

    public String getBoroughName() {
        if (properties.containsKey("borough")) {
            return properties.get("borough").toString();
        } else {
            return "Unknown Borough";
        }
    }
}
