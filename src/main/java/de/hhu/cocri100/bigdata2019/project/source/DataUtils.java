package de.hhu.cocri100.bigdata2019.project.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class DataUtils {

    public static final String dataRoot = "data";
    public static final String gogodRoot = "data/GoGoD";
    public static final String kgsRoot = "data/KGS";

    public static String[] getGogodFileNames() {
        try (Stream<Path> walk = Files.walk(Paths.get(gogodRoot))) {
            return walk
                    .map(Path::toString)
                    .filter(f -> !f.contains("Non") && f.endsWith(".sgf"))
                    .toArray(String[]::new);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String[] getKgsFileNames() {
        try (Stream<Path> walk = Files.walk(Paths.get(kgsRoot))) {
            return walk
                    .map(Path::toString)
                    .filter(f -> f.contains("-19-") && f.endsWith(".sgf"))
                    .toArray(String[]::new);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Boolean checkFileExists(String path) {
        File file = new File(path);
        return file.exists();
    }

    public static final String batchGameLengthStatsResultFileName = "results/GameLengthStatsBatch.txt";
    public static final String batchWinnerStatsResultFileName = "results/WinnerStatsBatch.txt";
    public static final String streamGameLengthStatsResultFileName = "results/GameLengthStatsStream.txt";
    public static final String streamWinnerStatsResultFileName = "results/WinnerStatsStream.txt";

    public static HashMap<Tuple2<String, String>, Tuple3<String, String, Integer>> readBatchGameLengthStats() throws IOException {
        Path p = Paths.get(batchGameLengthStatsResultFileName);

        HashMap<Tuple2<String, String>, Tuple3<String, String, Integer>> tuplesMap = new HashMap<>();

        if (Files.exists(p)) {
            List<String> lines = Files.readAllLines(p);

            for (String line: lines) {
                String[] tmp = line.trim().substring(1, line.length() - 1).split(",");

                String datasetName = tmp[0];
                String stat = tmp[1];
                Integer value = Integer.parseInt(tmp[2]);

                tuplesMap.put(new Tuple2<>(datasetName, stat), new Tuple3<>(datasetName, stat, value));
            }

            return tuplesMap;
        } else {
            throw new FileNotFoundException(p.toString());
        }
    }

    public static HashMap<Tuple2<String, String>, Tuple3<String, String, Float>> readBatchWinnerStats() throws IOException {
        Path p = Paths.get(batchWinnerStatsResultFileName);

        HashMap<Tuple2<String, String>, Tuple3<String, String, Float>> tuplesMap = new HashMap<>();

        if (Files.exists(p)) {
            List<String> lines = Files.readAllLines(p);

            for (String line: lines) {
                String[] tmp = line.trim().substring(1, line.length() - 1).split(",");

                String datasetName = tmp[0];
                String stat = tmp[1];
                Float value = Float.parseFloat(tmp[2]);

                tuplesMap.put(new Tuple2<>(datasetName, stat), new Tuple3<>(datasetName, stat, value));
            }

            return tuplesMap;
        } else {
            throw new FileNotFoundException(p.toString());
        }
    }
}
