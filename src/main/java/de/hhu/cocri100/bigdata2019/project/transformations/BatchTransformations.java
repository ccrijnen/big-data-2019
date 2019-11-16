package de.hhu.cocri100.bigdata2019.project.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchTransformations {

    public static final class CountGamesMapper
            implements MapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(Tuple2<String, String> value) {
            return new Tuple2<>(value.f0, 1);
        }
    }

    public static final class CountWinnerMapper
            implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> map(Tuple2<String, String> value) {
            return new Tuple3<>(value.f0, value.f1, 1);
        }
    }

    public static final class CalculateWinPercentageMapper
            implements MapFunction<Tuple2<Tuple3<String, String, Integer>, Tuple2<String, Integer>>, Tuple3<String, String, Float>> {
        @Override
        public Tuple3<String, String, Float> map(Tuple2<Tuple3<String, String, Integer>, Tuple2<String, Integer>> value) {
            return new Tuple3<>(value.f0.f0, value.f0.f1, value.f0.f2.floatValue() / value.f1.f1);
        }
    }

}
