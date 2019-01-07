/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.heron.examples.streamlet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.heron.examples.streamlet.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.BuilderImpl;

/**
 * This is a very simple topology that shows a series of streamlet operations
 * on a source streamlet of random integers (between 1 and 10). First, 1 is added
 * to each integer. That streamlet is then united with a streamlet that consists
 * of an indefinite stream of zeroes. At that point, all 2s are excluded from the
 * streamlet. The final output of the processing graph is then logged.
 */
public final class ComplexSourceAckingTopology {

  private static final Logger LOG = Logger.getLogger(ComplexSourceAckingTopology.class.getName());

  private ComplexSourceAckingTopology() {
  }

  // Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  private static boolean useSimulator = true;

  private static class IntegerSource implements Source<Integer> {

    Random rnd = new Random();
    List<Integer> intList;

    IntegerSource() {
      intList = new ArrayList<>();
    }

    /**
     * The setup functions defines the instantiation logic for the source.
     */
    public void setup(Context context) {
    }

    /**
     * The get function defines how elements for the source streamlet are
     * gotten.
     */
    public Collection<Integer> get() {
      intList.clear();
      intList.add(rnd.nextInt(10) + 1);
      intList.add(rnd.nextInt(10) + 1);
      intList.add(rnd.nextInt(10) + 1);
      StreamletUtils.sleep(1000);
      return intList;
    }

    public void cleanup() {
    }
  }


  private static class ComplexIntegerSink<T> implements Sink<T> {
    private static final long serialVersionUID = -96514621878356324L;

    ComplexIntegerSink() {
    }

    /**
     * The setup function is called before the sink is used. Any complex
     * instantiation logic for the sink should go here.
     */
    public void setup(Context context) {
    }

    /**
     * The put function defines how each incoming streamlet element is
     * actually processed. In this case, each incoming element is converted
     * to a byte array and written to the temporary file (successful writes
     * are also logged). Any exceptions are converted to RuntimeExceptions,
     * which will effectively kill the topology.
     */
    public void put(T element) {
      LOG.info(">>>> element: " + element);
    }

    /**
     * Any cleanup logic for the sink can be applied here.
     */
    public void cleanup() {
    }
  }

  /**
   * All Heron topologies require a main function that defines the topology's behavior
   * at runtime
   */
  public static void main(String[] args) throws Exception {

    if (args != null && args.length > 0) {
      useSimulator = false;
    }
    LOG.info(">>>> ****** useSimulator : " + useSimulator);

    Builder builder = Builder.newBuilder();

    Source<Integer> integerSource = new IntegerSource();

    builder.newSource(integerSource)
        .setName("integer-source")
        .map(i -> i*100)
        .toSink(new ComplexIntegerSink<>());

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
        .build();

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    if (useSimulator) {
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    } else {
      // Fetches the topology name from the first command-line argument
      String topologyName = StreamletUtils.getTopologyName(args);
      // Finally, the processing graph and configuration are passed to the Runner, which converts
      // the graph into a Heron topology that can be run in a Heron cluster.
      new Runner().run(topologyName, config, builder);
    }
  }
}
