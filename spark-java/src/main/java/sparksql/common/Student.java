package sparksql.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Student {
    String name;
    Integer age;
    Double gpa;
}