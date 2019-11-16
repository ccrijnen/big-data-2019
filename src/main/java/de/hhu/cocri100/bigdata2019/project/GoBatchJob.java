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

import de.hhu.cocri100.bigdata2019.project.data.GameStatsGenerator;
import de.hhu.cocri100.bigdata2019.project.transformations.Transformations;
import de.hhu.cocri100.bigdata2019.project.data.DataUtils;
import de.hhu.cocri100.bigdata2019.project.data.GameStats;
import de.hhu.cocri100.bigdata2019.project.transformations.BatchTransformations;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import java.util.*;

import static de.hhu.cocri100.bigdata2019.project.data.DataUtils.*;
import static de.hhu.cocri100.bigdata2019.project.data.DataUtils.gogodRoot;


/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class GoBatchJob {

    public static void main(String[] args) throws Exception {
        GoBatchJob bj = new GoBatchJob();
        bj.offlineAnalysis();
    }

    // set up the batch execution environment
    private final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    private Double dataFraction;
	private DataSet<GameStats> goDataSet;

    public GoBatchJob() throws Exception {
        this(0.01);
    }

    public GoBatchJob(Double dataFraction) throws Exception {
        if (0 < dataFraction && dataFraction <= 1) {
            this.dataFraction = dataFraction;
        } else {
            throw new Exception(String.format("dataFraction must be in range 0 < dataFraction <= 1 but found %f", dataFraction));
        }

		setupDataSet();
	}

    public void offlineAnalysis() throws Exception {
	    getGameLengthStats();
        getWinnerStats();
    }

    /*
     * GAME LENGTH STATS
     */

    private void getGameLengthStats() throws Exception {

        DataSet<Tuple2<String, Integer>> gameLengthData = goDataSet
                .map(new Transformations.SelectNameAndGameLengthMapper());

        // count the number of games in each dataset
        DataSet<Tuple3<String, String, Integer>> num = gameLengthData
                .map(new Transformations.NumGameMapper("count"))
                .groupBy(0)
                .sum(2);

        // find the max game length in each dataset
        DataSet<Tuple3<String, String, Integer>> max = gameLengthData
                .map(new Transformations.EnrichInfoMapper("max"))
                .groupBy(0)
                .max(2);

        // find the min game length in each dataset
        DataSet<Tuple3<String, String, Integer>> min = gameLengthData
                .map(new Transformations.EnrichInfoMapper("min"))
                .groupBy(0)
                .min(2);

        // calculate the mean game length in each dataset
        DataSet<Tuple3<String, String, Integer>> mean = gameLengthData
                .map(new Transformations.EnrichMeanMapper())
                .groupBy(0)
                .sum(2)
                .andSum(3)
                .map(new Transformations.CalculateMeanMapper());

        // unify results
        DataSet<Tuple3<String, String, Integer>> gameLengthResults = num
                .union(max)
                .union(min)
                .union(mean)
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1);

        gameLengthResults.print();
        gameLengthResults.writeAsText(DataUtils.batchGameLengthStatsResultFileName, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }

    /*
     * WINNER STATS
     */

    private void getWinnerStats() throws Exception {

        DataSet<Tuple2<String, String>> winnerData = goDataSet
                .map(new Transformations.SelectNameAndWinnerMapper());

        DataSet<Tuple2<String, Integer>> total = winnerData
                .map(new BatchTransformations.CountGamesMapper())
                .groupBy(0)
                .sum(1);

        DataSet<Tuple3<String, String, Integer>> winners = winnerData
                .map(new BatchTransformations.CountWinnerMapper())
                .groupBy(0, 1)
                .sum(2);

        DataSet<Tuple3<String, String, Float>> winnerResults = winners
                .join(total).where(0).equalTo(0)
                .map(new BatchTransformations.CalculateWinPercentageMapper())
                .sortPartition(0, Order.ASCENDING)
                .setParallelism(1)
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1);

        winnerResults.print();
        winnerResults.writeAsText(DataUtils.batchWinnerStatsResultFileName, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        // execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    /*
     * DATASET SETUP
     */

    private void setupDataSet() {
        // find all KGS *.sgf file names
        String[] kgsFileNames = getKgsFileNames();

        this.goDataSet = getDataSetFromFileNames(kgsFileNames);

        if (checkFileExists(gogodRoot)) {
            // find all GoGoD *.sgf file names
            String[] gogodFileNames = getGogodFileNames();

            DataSet<GameStats> gogodDataSet = getDataSetFromFileNames(gogodFileNames);

            // create union of both data sets
            this.goDataSet = goDataSet.union(gogodDataSet);
        }
    }

    private DataSet<GameStats> getDataSetFromFileNames(String[] fileNames) {
        assert fileNames != null;

        // shuffle filenames with random seed to make results reproducible
        List<String> fileNamesList = Arrays.asList(fileNames);
        Collections.shuffle(fileNamesList, new Random(42));
        String[] shuffledFileNames = (String[]) fileNamesList.toArray();

        // take a small subset of file names
        if (dataFraction < 1) {
            shuffledFileNames = Arrays.copyOfRange(shuffledFileNames, 0, (int) (dataFraction * shuffledFileNames.length));
        }

        // read GamesStatsTuple from sgf files
        ArrayList<GameStats> gameStats = new ArrayList<>(shuffledFileNames.length);
        GameStatsGenerator generator = new GameStatsGenerator();
        for (String fileName: shuffledFileNames) {
            gameStats.add(generator.fromSgf(fileName));
        }

        // load DataSet
        return env.fromCollection(gameStats);
    }
}
