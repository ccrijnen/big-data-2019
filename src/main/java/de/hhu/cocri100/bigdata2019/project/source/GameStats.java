package de.hhu.cocri100.bigdata2019.project.source;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

public class GameStats implements Serializable {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public String fileName;
    public String datasetName;
    public Integer gameLength;
    public String winner;
    public DateTime dateTime;

    public GameStats(String fileName, String datasetName, Integer gameLength, String winner, DateTime dateTime) {
        this.fileName = fileName;
        this.datasetName = datasetName;
        this.gameLength = gameLength;
        this.winner = winner;
        this.dateTime = dateTime;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(fileName).append(",");
        sb.append(datasetName).append(",");
        sb.append(gameLength).append(",");
        sb.append(winner).append(",");
        sb.append(dateTime.toString(timeFormatter)).append(")");

        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof GameStats &&
                this.fileName.equals(((GameStats) other).fileName);
    }

    @Override
    public int hashCode() {
        return this.fileName.hashCode();
    }

    public long getEventTime() {
        return dateTime.getMillis();
    }
}
