package com.study.flink.model;

public class SensorReading {
    private String name;
    private Long timestap;
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String name, Long timestap, Double temperature) {
        this.name = name;
        this.timestap = timestap;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", timestap=" + timestap +
                ", temperature=" + temperature +
                '}';
    }
}
