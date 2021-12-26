package com.study.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SensorReading {
    private String name;
    private Long timestamp;
    private Double temperature;

    public SensorReading() {
    }



    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", timestap=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

}
