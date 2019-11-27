# Big Data 2019 - Go Game Records Project

In this project we are analyzing Go games using using [Flink 1.9](https://ci.apache.org/projects/flink/flink-docs-release-1.9/), 
a popular big data framework for both stream and batch analytics.

The data for this project are game records of the board game Go. 
Two datasets were used, the [KGS dataset](https://www.u-go.net/gamerecords/), 
a collection of game records of top amateur games played on the [K Go Server (KGS)](http://www.gokgs.com/), 
and the [GoGoD dataset](https://gogodonline.co.uk/), a collection of game records of professional games.

## System prerequisites

- Maven 3.5 or higher
- JDK >= 1.8
- IDE like IntelliJ or Eclipse
- Apache Flink

## Setup

1. Clone the project with `git clone ...`
2. Build the project with `mvn clean package`
3. There are different classes with a main method
    - [Main.java](src/main/java/de/hhu/cocri100/bigdata2019/project/Main.java) - 
    CLI: Main interface to run the tasks
    - [DownloadKgsData.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/DownloadKgsData.java) - 
    download KGS dataset
    - [GoBatchJob.java](src/main/java/de/hhu/cocri100/bigdata2019/project/GoBatchJob.java) - 
    batch processing
    - [GoStreamingJob.java](src/main/java/de/hhu/cocri100/bigdata2019/project/GoStreamingJob.java) - 
    stream processing (analyze, compare and predict)

## Overview

The structure is organized as follows:

- [de.hhu.cocri100.bigdata2019.project.source](src/main/java/de/hhu/cocri100/bigdata2019/project/source) - 
all data processing related features
- [de.hhu.cocri100.bigdata2019.project.transformations](src/main/java/de/hhu/cocri100/bigdata2019/project/transformations) - 
all inherited classes (maps, reduces, sinks, etc.)
    - [Transformations.java](src/main/java/de/hhu/cocri100/bigdata2019/project/transformations/Transformations.java) - 
    transformations used in both batch and stream processing
    - [BatchTransformations.java](src/main/java/de/hhu/cocri100/bigdata2019/project/transformations/BatchTransformations.java) - 
    transformations used only in batch processing
    - [StreamingTransformation.java](src/main/java/de/hhu/cocri100/bigdata2019/project/transformations/StreamingTransformations.java) - 
    transformations used only in stream processing

### Data

Each game record in the GoGoD and KGS dataset is saved as a file in the smart game format (sgf).
A game record contains the moves of the game in a tree structure with additional properties 
(e.g. the winner of the game, the player names, etc).
To parse a game record to a POJO object [sgf4j](https://github.com/toomasr/sgf4j) is used.

- [DownloadKgsData.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/DownloadKgsData.java) - 
(String) _dataRoot_ optional parameter, default: `data/`, 
downloads the KGS archives to `dataRoot/raw` and then unzips them to `dataRoot/KGS`
- [GameStats.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/GameStats.java) - 
POJO to store a Go record
    - _fileName_ - (String) id of the object
    - _datasetName_ - (String) name of the dataset (KGS or GoGoD)
    - _gameLength_ - (Integer) number of moves in the game record
    - _winner_ - (String) winner of the game (black/white/none)
    - _dateTime_ - (DateTime) random date and time used to model event time stream processing
- [GameStatsGenerator.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/GameStatsGenerator.java) - 
reads Go records from a sgf file, generates a random time stamp and returns a GameStats object
- [GameStatsSource.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/GameStatsSource.java) - 
simulates a streaming source with delayed events
- [DataUtils.java](src/main/java/de/hhu/cocri100/bigdata2019/project/source/DataUtils.java) - data related utils

**Since the GoGoD dataset is a commercial dataset it can't be directly downloaded.
The data collection will therefore only download the KGS dataset**.
If statistics on the GoGoD dataset are also desired, it needs to be manually downloaded and extracted into the folder `data/GoGoD`.

At the beginning of this project, the decision was made to cover two aspects of the game records, the game length and the winner.
The following statistics will be considered in the analysis and comparison tasks:

### Batch processing

The [GoBatchJob.java](src/main/java/de/hhu/cocri100/bigdata2019/project/GoBatchJob.java) 
file contains the class to run the offline analysis.

Optional constructor parameter:
- _dataFraction_ - (Double) fraction of the data to use, default 0.01

Upon instantiation will search for the paths to the game records in `data/KGS` and `data/GoGoD` (if available).
The game records (or a fraction of them) will then be parsed to `GameStats` objects, 
from which a Flink `DataSet` will be created for each dataset.
If both the KGS and GoGoD data are found the final `DataSet` will be a union of both.

#### Offline analysis

The `GoBatchJob.offlineAnalysis()` method calculates these Game Length and Winner [statistics](#offline-statistics).
The results of the offline analysis are saved to `results/GameLengthStatsBatch.txt` and `results/WinnerStatsBatch.txt`.

##### Offline statistics

Game Length statistics | Winner statistics
-----------------------|------------------
**count** - number of game records | **black** - percentage of black wins
**max** - max game length | **white** - percentage of white wins
**min** - min game length | **none** - percentage of draws or game records with unknown outcome
**mean** - average game length | 

### Stream processing

The [GoStreamingJob.java](src/main/java/de/hhu/cocri100/bigdata2019/project/GoStreamingJob.java) 
file contains the class to run the online analysis, comparison and prediction.

Optional constructor parameters:
- _dataFraction_ - (Double) fraction of the data to use, default 0.01
- _maxEventDelay_ - (Integer) max number of seconds events are out of order by in the simulated stream, default 60
- _servingSpeedFactor_ - (Integer) number of events served in 1 second, default 1800

Upon instantiation will search for the paths to the game records in `data/KGS` and `data/GoGoD` (if available).
The game records (or a fraction of them) will then be parsed to `GameStats` objects, 
from which a simulated Flink `DataStream` will be created for each dataset.
If both the KGS and GoGoD data are found the final `DataStream` will be a union of both.

#### Online analysis

The `GoStreamingJob.onlineAnalysis()` method calculates these Game Length and Winner [statistics](#online-statistics) 
**in a one hour window**.
The results of the online analysis are saved to `results/GameLengthStatsStream.txt` and `results/WinnerStatsStream.txt`.

##### Online statistics

Game Length statistics | Winner statistics
-----------------------|------------------
**count** - number of game records | **black** - percentage of black wins
**max** - max game length | **white** - percentage of white wins
**min** - min game length | **none** - percentage of draws or game records with unknown outcome
**mean** - average game length | 

#### Online comparison

Compare the [hourly online results](#online-analysis) with the [offline results](#offline-analysis) by running the 
`GoStreamingJob.onlineComparison()` method.
Throws an error if the offline results can't be found in `results/`.

##### Compared statistics

Game Length statistics | Winner statistics
-----------------------|------------------
**count** - number of game records | **black** - percentage of black wins
**max** - max game length | **white** - percentage of white wins
**min** - min game length | **none** - percentage of draws or game records with unknown outcome
**mean** - average game length | 

#### Online prediction

The following predictions are calculated by running the `GoStreamingJob.onlinePrediction()` method.
These statistics are calculated using running averages.

##### Predicted statistics

Game Length statistics | Winner statistics
-----------------------|------------------
**mean** - average game length | **black** - percentage of black wins
- | **white** - percentage of white wins
- | **none** - percentage of draws or game records with unknown outcome


### Visualization

You can find the results of the offline and online analysis here:
- [game length batch results](results/GameLengthStatsBatch.txt)
- [game length streaming results](results/GameLengthStatsStream.txt)
- [winner batch results](results/WinnerStatsBatch.txt)
- [winner streaming results](results/WinnerStatsStream.txt)
