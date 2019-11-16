package de.hhu.cocri100.bigdata2019.project.transformations;

import de.hhu.cocri100.bigdata2019.project.source.GameStats;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;


public class Transformations {

    public static final class SelectNameAndGameLengthMapper
            implements MapFunction<GameStats, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(GameStats value) {
            return new Tuple2<>(value.datasetName, value.gameLength);
        }
    }


    public static final class NumGameMapper
            implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
        String info;

        public NumGameMapper(String info) {
            this.info = info;
        }

        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, Integer> value) {
            return new Tuple3<>(value.f0, info, 1);
        }
    }


    public static final class EnrichInfoMapper
            implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
        String info;

        public EnrichInfoMapper(String info) {
            this.info = info;
        }

        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, Integer> value) {
            return new Tuple3<>(value.f0, info, value.f1);
        }
    }


    public static final class EnrichMeanMapper
            implements MapFunction<Tuple2<String, Integer>, Tuple4<String, String, Integer, Integer>> {
        @Override
        public Tuple4<String, String, Integer, Integer> map(Tuple2<String, Integer> value) {
            return new Tuple4<>(value.f0, "mean", value.f1, 1);
        }
    }


    public static final class CalculateMeanMapper
            implements MapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> map(Tuple4<String, String, Integer, Integer> value) {
            return new Tuple3<>(value.f0, value.f1, value.f2 / value.f3);
        }
    }


    public static final class SelectNameAndWinnerMapper
            implements MapFunction<GameStats, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> map(GameStats value) {
            return new Tuple2<>(value.datasetName, value.winner);
        }
    }
}
