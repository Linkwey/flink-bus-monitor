package com.bus.model;

import java.io.Serializable;
import java.sql.Timestamp;

public class BusGPSRecord implements Serializable {
    private String chehao;      // 车号
    private Timestamp gpsTime;  // GPS时间
    private double lat;         // 纬度（WGS84）
    private double lng;         // 经度（WGS84）
    private double speed;       // 速度 km/h
    private String line;        // 线路
    private String station;     // 站点名称（未使用）

    public BusGPSRecord() {}

    public BusGPSRecord(String chehao, Timestamp gpsTime, double lat, double lng,
                        double speed, String line, String station) {
        this.chehao = chehao;
        this.gpsTime = gpsTime;
        this.lat = lat;
        this.lng = lng;
        this.speed = speed;
        this.line = line;
        this.station = station;
    }

    public String getChehao() { return chehao; }
    public void setChehao(String chehao) { this.chehao = chehao; }

    public Timestamp getGpsTime() { return gpsTime; }
    public void setGpsTime(Timestamp gpsTime) { this.gpsTime = gpsTime; }

    public double getLat() { return lat; }
    public void setLat(double lat) { this.lat = lat; }

    public double getLng() { return lng; }
    public void setLng(double lng) { this.lng = lng; }

    public double getSpeed() { return speed; }
    public void setSpeed(double speed) { this.speed = speed; }

    public String getLine() { return line; }
    public void setLine(String line) { this.line = line; }

    public String getStation() { return station; }
    public void setStation(String station) { this.station = station; }
}