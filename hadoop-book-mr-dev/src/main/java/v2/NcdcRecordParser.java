package v2;

import org.apache.hadoop.io.Text;

/**
 * @author: zhangyachong1
 * @date: 2019-09-02
 * @description:
 */
public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;
    private String stationId;
    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString;
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }
    public void parse(Text record) {
        parse(record.toString());
    }
    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE &&
                quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getStationId() {
        return stationId;
    }
}
