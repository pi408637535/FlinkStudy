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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestap() {
        return timestap;
    }

    public void setTimestap(Long timestap) {
        this.timestap = timestap;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
