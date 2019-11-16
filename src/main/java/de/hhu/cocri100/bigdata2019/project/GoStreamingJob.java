/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.hhu.cocri100.bigdata2019.project;

import de.hhu.cocri100.bigdata2019.project.source.DataUtils;
import de.hhu.cocri100.bigdata2019.project.source.GameStats;
import de.hhu.cocri100.bigdata2019.project.source.GameStatsGenerator;
import de.hhu.cocri100.bigdata2019.project.source.GameStatsSource;
import de.hhu.cocri100.bigdata2019.project.transformations.Transformations;
import de.hhu.cocri100.bigdata2019.project.transformations.StreamingTransformations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.*;

import static de.hhu.cocri100.bigdata2019.project.source.DataUtils.*;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class GoStreamingJob {

	public static void main(String[] args) throws Exception {
		GoStreamingJob sj = new GoStreamingJob();
//		sj.onlineAnalysis();
//		sj.onlineCompare();
		sj.onlinePrediction();
	}

	// set up the batch execution environment
	private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private Double dataFraction;
	private DataStream<GameStats> goDataStream;

    // max number of seconds events are out of order by
    private final int maxEventDelay;
    // number of events served in 1 second
    private final int servingSpeedFactor;

	public GoStreamingJob() throws IllegalArgumentException {
		this(0.01, 60, 1800);
	}

    public GoStreamingJob(Double dataFraction) throws IllegalArgumentException {
        this(dataFraction, 60, 1800);
    }

	public GoStreamingJob(Double dataFraction, Integer maxEventDelay, Integer servingSpeedFactor) throws IllegalArgumentException {
        if (!(0 < dataFraction && dataFraction <= 1)) {
            throw new IllegalArgumentException(String.format("Data fraction must be in range 0 < dataFraction <= 1 but found %f", dataFraction));
        }
        if(maxEventDelay < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        this.dataFraction = dataFraction;
		this.maxEventDelay = maxEventDelay;
		this.servingSpeedFactor = servingSpeedFactor;

		getDataStreams();
	}

	public enum  Mode {
		ANALYSIS,
		COMPARE,
		PREDICT
	}

	public void onlineAnalysis() throws Exception {
		Mode mode = Mode.ANALYSIS;
		getGameLengthStats(mode);
		getWinnerStats(mode);
	}

	public void onlineComparison() throws Exception {
		Mode mode = Mode.COMPARE;
		getGameLengthStats(mode);
		getWinnerStats(mode);
	}

	public void onlinePrediction() throws Exception {
		Mode mode = Mode.PREDICT;
		getGameLengthStats(mode);
		getWinnerStats(mode);
	}

	/*
	 * GAME LENGTH STATS PER HOUR
	 */

	private void getGameLengthStats(Mode mode) throws Exception {

		DataStream<Tuple2<String, Integer>> gameLengthData = goDataStream
				.map(new Transformations.SelectNameAndGameLengthMapper());

		// count the number of games in each dataset
		DataStream<Tuple3<String, String, Integer>> num = gameLengthData
				.map(new Transformations.NumGameMapper("count"))
				.keyBy(0)
				.timeWindow(Time.hours(1))
				.sum(2);

		// find the max game length in each dataset
		DataStream<Tuple3<String, String, Integer>> max = gameLengthData
				.map(new Transformations.EnrichInfoMapper("max"))
				.keyBy(0)
				.timeWindow(Time.hours(1))
				.max(2);

		// find the min game length in each dataset
		DataStream<Tuple3<String, String, Integer>> min = gameLengthData
				.map(new Transformations.EnrichInfoMapper("min"))
				.keyBy(0)
				.timeWindow(Time.hours(1))
				.min(2);

		// calculate the mean game length in each dataset
		DataStream<Tuple3<String, String, Integer>> mean = gameLengthData
				.map(new Transformations.EnrichMeanMapper())
				.keyBy(0)
				.timeWindow(Time.hours(1))
				.reduce(new StreamingTransformations.SumField2AndField3Reducer())
				.map(new Transformations.CalculateMeanMapper());

		// unify the results in one stream
		DataStream<Tuple3<String, String, Integer>> gameLengthResults = num
				.union(max)
				.union(min)
				.union(mean);

		switch (mode) {
			case ANALYSIS:
				gameLengthResults.print();
				gameLengthResults.writeAsText(DataUtils.streamGameLengthStatsResultFileName, FileSystem.WriteMode.OVERWRITE)
						.setParallelism(1);
				break;

			case COMPARE:
				gameLengthResults.addSink(new StreamingTransformations.CompareGameLengthStatsSink()).setParallelism(1);
				break;

			case PREDICT:
				mean.print();

				gameLengthData
						.map(new Transformations.EnrichMeanMapper())
						.keyBy(0)
						.timeWindow(Time.hours(1))
						.reduce(new StreamingTransformations.SumField2AndField3Reducer())
						.addSink(new StreamingTransformations.PredictMeanGameLengthSink())
						.setParallelism(1);
				break;
		}

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	/*
	 * WINNER STATS PER HOUR
	 */

	private void getWinnerStats(Mode mode) throws Exception {

		DataStream<Tuple2<String, String>> winnerData = goDataStream
				.map(new Transformations.SelectNameAndWinnerMapper());

		DataStream<Tuple3<String, String, Float>> winnerResults = winnerData
				.keyBy(value -> value.f0)
				.timeWindow(Time.hours(1))
				.apply(new StreamingTransformations.WinPercentagesWindowFunction());

		switch (mode) {
			case ANALYSIS:
				winnerResults.print();
				winnerResults.writeAsText(DataUtils.streamWinnerStatsResultFileName, FileSystem.WriteMode.OVERWRITE)
						.setParallelism(1);
				break;

			case COMPARE:
				winnerResults.addSink(new StreamingTransformations.CompareWinnerStatsSink()).setParallelism(1);
				break;

			case PREDICT:
				winnerResults.print();

				winnerData
						.keyBy(value -> value.f0)
						.timeWindow(Time.hours(1))
						.apply(new StreamingTransformations.PredictWinnersWindowFunction())
						.addSink(new StreamingTransformations.PredictWinnersSink())
						.setParallelism(1);
				break;
		}

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	/*
	 * DATASTREAM SETUP
	 */

	private void getDataStreams() {
		// find all KGS *.sgf file names
		String[] kgsFileNames = getKgsFileNames();

		this.goDataStream = dataStreamFromFileNames(kgsFileNames);

		if (checkFileExists(gogodRoot)) {
			// find all GoGoD *.sgf file names
			String[] gogodFileNames = getGogodFileNames();

			DataStream<GameStats> gogodDataStream = dataStreamFromFileNames(gogodFileNames);

			// create union of both data sets
			this.goDataStream = goDataStream.union(gogodDataStream);
		}
	}

	private DataStream<GameStats> dataStreamFromFileNames(String[] fileNames) {
		assert fileNames != null;

		// shuffle filenames with random seed to make results reproducible
		List<String> fileNamesList = Arrays.asList(fileNames);
		Collections.shuffle(fileNamesList, new Random(69));
		String[] shuffledFileNames = (String[]) fileNamesList.toArray();

		// take a small subset of file names
		if (dataFraction < 1) {
			shuffledFileNames = Arrays.copyOfRange(shuffledFileNames, 0, (int) (dataFraction * shuffledFileNames.length));
		}

		// read GamesStatsTuple from sgf files
		GameStatsGenerator generator = new GameStatsGenerator();
		ArrayList<GameStats> gameStats = new ArrayList<>(shuffledFileNames.length);

		for (String fileName: shuffledFileNames) {
			gameStats.add(generator.fromSgf(fileName));
		}

		// start the data generator
		return env.addSource(new GameStatsSource(gameStats, maxEventDelay, servingSpeedFactor));
	}
}
