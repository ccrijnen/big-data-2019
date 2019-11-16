package de.hhu.cocri100.bigdata2019.project.transformations;

import de.hhu.cocri100.bigdata2019.project.source.DataUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class StreamingTransformations {

    public static final class SumField2AndField3Reducer
            implements ReduceFunction<Tuple4<String, String, Integer, Integer>> {
        @Override
        public Tuple4<String, String, Integer, Integer> reduce(
                Tuple4<String, String, Integer, Integer> value1, Tuple4<String, String, Integer, Integer> value2) {
            return new Tuple4<>(value1.f0, value1.f1, value1.f2 + value2.f2, value1.f3 + value2.f3);
        }
    }

    public static final class WinPercentagesWindowFunction implements
            WindowFunction<Tuple2<String, String>, Tuple3<String, String, Float>, String, TimeWindow> {
        @Override
        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple2<String, String>> games,
                          Collector<Tuple3<String, String, Float>> out) {

            int count = 0;
            int black = 0;
            int white = 0;
            int none = 0;

            for(Tuple2<String, String> game : games) {
                if (game.f1.equals("black")) {
                    black++;
                } else if (game.f1.equals("white")) {
                    white++;
                } else {
                    none++;
                }

                count++;
            }

            out.collect(new Tuple3<>(key, "black", (float) black / count));
            out.collect(new Tuple3<>(key, "white", (float) white / count));
            out.collect(new Tuple3<>(key, "none", (float) none / count));
        }
    }

    public static final class CompareGameLengthStatsSink extends RichSinkFunction<Tuple3<String, String, Integer>> {

        private static final long serialVersionUID = 1L;

        private HashMap<Tuple2<String, String>, Tuple3<String, String, Integer>> stats;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.stats = DataUtils.readBatchGameLengthStats();
        }

        @Override
        public void invoke(Tuple3<String, String, Integer> value, Context context) {
            String datasetName = value.f0;
            String stat = value.f1;
            Integer val = value.f2;

            Tuple3<String, String, Integer> statTuple = stats.get(new Tuple2<>(datasetName, stat));

            if (statTuple != null) {
                System.out.println(String.format( "stream-1h-stat: %-20s, batch-stat: %-20s, diff: %s", value, statTuple, Math.abs(val - statTuple.f2)));
            }
        }
    }

    public static final class CompareWinnerStatsSink extends RichSinkFunction<Tuple3<String, String, Float>> {

        private static final long serialVersionUID = 1L;

        private HashMap<Tuple2<String, String>, Tuple3<String, String, Float>> stats;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.stats = DataUtils.readBatchWinnerStats();
        }

        @Override
        public void invoke(Tuple3<String, String, Float> value, Context context) {
            String datasetName = value.f0;
            String stat = value.f1;
            Float val = value.f2;

            Tuple3<String, String, Float> statTuple = stats.get(new Tuple2<>(datasetName, stat));

            if (statTuple != null) {
                System.out.println(String.format( "stream-1h-stat: %-25s, batch-stat: %-20s, diff: %s", value, statTuple, Math.abs(val - statTuple.f2)));
            }
        }
    }

    public static final class PredictMeanGameLengthSink extends RichSinkFunction<Tuple4<String, String, Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private HashMap<String, Tuple2<Long, Long>> sumsAndCounts = new HashMap<>();

        @Override
        public void open(Configuration parameters) {
            this.sumsAndCounts.put("KGS", new Tuple2<>(0L, 0L));
            this.sumsAndCounts.put("GoGoD", new Tuple2<>(0L, 0L));
        }

        @Override
        public void invoke(Tuple4<String, String, Integer, Integer> value, Context context) {
            String datasetName = value.f0;

            Integer curSum = value.f2;
            Integer curCount = value.f3;

            Tuple2<Long, Long> curSumAndCount = sumsAndCounts.get(datasetName);

            if (curSumAndCount != null) {

                Long sum = curSum + curSumAndCount.f0;
                Long count = curCount + curSumAndCount.f1;
                Tuple2<Long, Long> sumAndCount = new Tuple2<>(sum, count);

                sumsAndCounts.replace(datasetName, sumAndCount);

                System.out.println("Predicted " + datasetName + " mean game length for the next hour: " + sum / count);
            }
        }
    }

    public static final class PredictWinnersWindowFunction implements WindowFunction<
            Tuple2<String, String>,
            Tuple3<String, Tuple3<Integer, Integer, Integer>, Integer>,
            String,
            TimeWindow> {
        @Override
        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple2<String, String>> games,
                          Collector<Tuple3<String, Tuple3<Integer, Integer, Integer>, Integer>> out) {

            int count = 0;
            int black = 0;
            int white = 0;
            int none = 0;

            for(Tuple2<String, String> game : games) {
                if (game.f1.equals("black")) {
                    black++;
                } else if (game.f1.equals("white")) {
                    white++;
                } else {
                    none++;
                }

                count++;
            }

            out.collect(new Tuple3<>(key, new Tuple3<>(black, white, none), count));
        }
    }

    public static final class PredictWinnersSink
            extends RichSinkFunction<Tuple3<String, Tuple3<Integer, Integer, Integer>, Integer>> {

        private static final long serialVersionUID = 1L;

        private HashMap<String, Tuple3<Integer, Integer, Integer>> countsMap = new HashMap<>();
        private HashMap<String, Integer> totalsMap = new HashMap<>();

        @Override
        public void open(Configuration parameters) {
            this.countsMap.put("KGS", new Tuple3<>(0, 0, 0));
            this.countsMap.put("GoGoD", new Tuple3<>(0, 0, 0));

            this.totalsMap.put("KGS", 0);
            this.totalsMap.put("GoGoD", 0);
        }

        private static Tuple3<Integer, Integer, Integer> addTuple3(Tuple3<Integer, Integer, Integer> val1,
                                                                   Tuple3<Integer, Integer, Integer> val2) {
            return new Tuple3<>(val1.f0 + val2.f0, val1.f1 + val2.f1, val1.f2 + val2.f2);
        }


        @Override
        public void invoke(Tuple3<String, Tuple3<Integer, Integer, Integer>, Integer> value, Context context) {
            String datasetName = value.f0;

            Tuple3<Integer, Integer, Integer> curCounts = value.f1;
            Integer curTotal = value.f2;

            Tuple3<Integer, Integer, Integer> oldCounts = countsMap.get(datasetName);
            Integer oldTotal = totalsMap.get(datasetName);

            if (oldCounts != null && oldTotal != null) {

                Tuple3<Integer, Integer, Integer> counts = addTuple3(oldCounts, curCounts);
                Integer total = oldTotal + curTotal;

                countsMap.replace(datasetName, counts);
                totalsMap.replace(datasetName, total);

                Float black = counts.f0.floatValue() / total;
                Float white = counts.f1.floatValue() / total;
                Float none = counts.f2.floatValue() / total;

                System.out.println("Predicted win percentages for the next hour: " + new Tuple3<>(datasetName, "black", black));
                System.out.println("Predicted win percentages for the next hour: " + new Tuple3<>(datasetName, "white", white));
                System.out.println("Predicted win percentages for the next hour: " + new Tuple3<>(datasetName, "none", none));
            }
        }
    }
}
