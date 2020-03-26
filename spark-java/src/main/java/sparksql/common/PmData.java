package sparksql.common;

import lombok.*;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PmData implements Serializable {
    Long id;
    Integer year;
    Integer month;
    Integer day;
    Integer hour;
    Integer season;
    Double pm;
}
