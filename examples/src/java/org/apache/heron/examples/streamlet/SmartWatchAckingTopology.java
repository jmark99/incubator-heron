package org.apache.heron.examples.streamlet;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class SmartWatchAckingTopology {

  private static boolean useSimulator = true;

  private SmartWatchAckingTopology() {
  }

  private static final Logger LOG =
      Logger.getLogger(SmartWatchAckingTopology.class.getName());

  private static final List<String> JOGGERS = Arrays.asList(
      "bill",
      "ted"
  );

  private static class SmartWatchReading implements Serializable {
    private static final long serialVersionUID = -6555650939020508026L;
    private final String joggerId;
    private final int feetRun;

    SmartWatchReading() {
      StreamletUtils.sleep(1000);
      this.joggerId = StreamletUtils.randomFromList(JOGGERS);
      this.feetRun = ThreadLocalRandom.current().nextInt(200, 400);
    }

    String getJoggerId() {
      return joggerId;
    }

    int getFeetRun() {
      return feetRun;
    }
  }

  public static void main(String[] args) throws Exception {

    if (args != null && args.length > 0) {
      useSimulator = false;
    }
    LOG.info(">>>> ****** useSimulator : " + useSimulator);

    Builder processingGraphBuilder = Builder.newBuilder();

    processingGraphBuilder.newSource(SmartWatchAckingTopology.SmartWatchReading::new)
        .setName("incoming-watch-readings")
        .reduceByKeyAndWindow(
            // Key extractor
            reading -> reading.getJoggerId(),
            // Value extractor
            reading -> reading.getFeetRun(),
            // The time window (1 minute of clock time)
            WindowConfig.TumblingTimeWindow(Duration.ofSeconds(10)),
            // The reduce function (produces a cumulative sum)
            (cumulative, incoming) -> cumulative + incoming
        )
        .setName("reduce-to-total-distance-per-jogger")
        .map(keyWindow -> {
          // The per-key result of the previous reduce step
          long totalFeetRun = keyWindow.getValue();

          // The amount of time elapsed
          long startTime = keyWindow.getKey().getWindow().getStartTime();
          long endTime = keyWindow.getKey().getWindow().getEndTime();
          long timeLengthMillis = endTime - startTime; // Cast to float to use as denominator

          // The feet-per-minute calculation
          float feetPerMinute = totalFeetRun / (float) (timeLengthMillis / 1000);

          // Reduce to two decimal places
          String paceString = new DecimalFormat("#.##").format(feetPerMinute);

          // Return a per-jogger average pace
          return new KeyValue<>(keyWindow.getKey().getKey(), paceString);
        })
        .setName("calculate-average-speed")
        .consume(kv -> {
          String logMessage = String.format("(runner: %s, avgFeetPerMinute: %s)",
              kv.getKey(),
              kv.getValue());

          LOG.info(logMessage);
        });

    //Config config = Config.defaultConfig();
    Config config = Config.newBuilder()
        .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
        .build();

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    if (useSimulator) {
      StreamletUtils.runInSimulatorMode((BuilderImpl) processingGraphBuilder, config);
    } else {
      // Fetches the topology name from the first command-line argument
      String topologyName = StreamletUtils.getTopologyName(args);
      // Finally, the processing graph and configuration are passed to the Runner, which converts
      // the graph into a Heron topology that can be run in a Heron cluster.
      new Runner().run(topologyName, config, processingGraphBuilder);
    }
  }
}
