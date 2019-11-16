package de.hhu.cocri100.bigdata2019.project.source;

import com.toomasr.sgf4j.Sgf;
import com.toomasr.sgf4j.parser.Game;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.Locale;

public class GameStatsGenerator {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    private DateTime startTime = DateTime.parse("2019-11-01 00:00:00", timeFormatter);

    private DateTime generateRandomDateTime() {
        long rangeBegin  = startTime.plus(Seconds.ONE).getMillis();
        long rangeEnd = startTime.plus(Minutes.TWO).getMillis();
        long diff = rangeEnd - rangeBegin + 1;
        long generatedLong = rangeBegin + (long) (Math.random() * diff);

        this.startTime = new DateTime(generatedLong).withMillisOfSecond(0);

        return this.startTime;
    }

    public GameStats fromSgf(String path) {
        String fileName = FileSystems.getDefault().getPath(path).normalize().toAbsolutePath().toString();
        Game game = Sgf.createFromPath(Paths.get(fileName));

        String datasetName = fileName.contains("KGS")? "KGS" : "GoGoD";

        Integer gameLength = game.getNoMoves();

        String winner;
        char w = game.getProperty("RE", "none").toLowerCase().charAt(0);
        if (w == 'b') {
            winner = "black";
        } else if (w == 'w') {
            winner = "white";
        } else {
            winner = "none";
        }

        return new GameStats(fileName, datasetName, gameLength, winner, generateRandomDateTime());
    }
}
