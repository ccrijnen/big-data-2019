package de.hhu.cocri100.bigdata2019.project.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.*;

public class GameStatsSource implements SourceFunction<GameStats> {

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final ArrayList<GameStats> gameStatsList;
    private final int servingSpeed;

    private transient Iterator gameStatsIterator;

    public GameStatsSource(ArrayList<GameStats> gameStatsList, int maxEventDelaySecs, int servingSpeedFactor) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }

        this.gameStatsList = gameStatsList;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = Math.max(maxDelayMsecs, 10000);
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<GameStats> sourceContext) throws Exception {
        this.gameStatsIterator = gameStatsList.iterator();

        generateUnorderedStream(sourceContext);

        this.gameStatsIterator = null;
    }

    private void generateUnorderedStream(SourceContext<GameStats> sourceContext) throws Exception {
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        // read first ride and insert it into emit schedule
        GameStats game;
        if (gameStatsIterator.hasNext() && (game = (GameStats) gameStatsIterator.next()) != null) {
            // extract starting timestamp
            dataStartTime = getEventTime(game);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, game));

            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        // peek at next ride
        if (gameStatsIterator.hasNext()) {
            game = (GameStats) gameStatsIterator.next();
        }

        // read rides one-by-one and emit a random ride from the buffer each time
        while (emitSchedule.size() > 0 || gameStatsIterator.hasNext()) {

            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = game != null ? getEventTime(game) : -1;

            // while there is a ride AND (no ride in schedule OR not enough rides in schedule)
            while (game != null && (emitSchedule.isEmpty() || rideEventTime < curNextDelayedEventTime + maxDelayMsecs)) {
                // insert event into emit schedule
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, game));

                // read next ride
                if (gameStatsIterator.hasNext() && (game = (GameStats) gameStatsIterator.next()) != null) {
                    rideEventTime = getEventTime(game);
                }
                else {
                    game = null;
                    rideEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep( (waitTime > 0) ? waitTime : 0);

            if(head.f1 instanceof GameStats) {
                GameStats emitGame = (GameStats) head.f1;
                // emit ride
                sourceContext.collectWithTimestamp(emitGame, getEventTime(emitGame));
            } else if(head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark) head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);

                if (emitSchedule.isEmpty() && !gameStatsIterator.hasNext()) {
                    // processed all files and ends the sourceContext after some time
                    sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
                } else {
                    // schedule next watermark
                    long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                    Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                    emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
                }
            }
        }
    }

    public long getEventTime(GameStats gameStats) {
        return gameStats.getEventTime();
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        if (this.gameStatsIterator != null) {
            this.gameStatsIterator = null;
        }
    }
}
