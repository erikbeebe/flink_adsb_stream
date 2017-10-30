package io.eventador;

// POJO to define plane schema

public class PlaneModel {
    public String flight;
    public String timestamp_verbose;
    public Integer msg_type;
    public Integer track;
    public Long timestamp;
    public Integer altitude;
    public Long counter;
    public Double lat;
    public Double lon;
    public String icao;
    public Integer speed;
    public String equipment;
    public String tail;

    public PlaneModel() {
        // empty constructor
    }

    public PlaneModel(String flight, String timestamp_verbose, Integer msg_type, Integer track, Long timestamp, Integer altitude, Long counter,
                      Double lat, Double lon, String icao, Integer speed, String tail, String equipment) {
        this.flight = flight;
        this.timestamp_verbose = timestamp_verbose;
        this.msg_type = msg_type;
        this.track = track;
        this.timestamp = timestamp;
        this.altitude = altitude;
        this.counter = counter;
        this.lat = lat;
        this.lon = lon;
        this.icao = icao;
        this.speed = speed;
        this.tail = tail;
        this.equipment = equipment;
    }

    public boolean isMilitary() {
        // Will identify some..
        return icao.startsWith("AE");
    };

    public String getEquipment() {
        return this.equipment;
    }

    public String getTailNumber() {
        return this.tail;
    }

    @Override
    public String toString() {
        return String.format("ICAO: %s Flight: %s Timestamp: %d Msg_type: %d Altitude: %d Speed: %d Lat: %f Lon: %f Counter: %d Equipment: %s Tail: %s", 
                icao, 
                flight, 
                timestamp, 
                msg_type, 
                altitude, 
                speed, 
                lat, 
                lon, 
                counter, 
                equipment, 
                tail);
    }
}
