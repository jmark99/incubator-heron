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

package org.apache.heron.streamlet.impl.operators;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.IOutputCollector;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.common.utils.tuple.TupleImpl;

public class MapOperatorTest {

  private List<Object> emittedTuples;
  private static final Logger LOG = Logger.getLogger(MapOperatorTest.class.getName());
  private int ackedTuples = 0;
  private Config config = new Config();

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test
  public void testMapOperatorWithAcking() {
    LOG.info(">>>> testing testMapOperatorWithAcking");
    ackedTuples = 0;
    MapOperator<Integer, Integer> mapOperator = getMapOperator(true);

    HashMap<Integer, Integer> expectedResults = new HashMap<>();
    expectedResults.put(12, 0);
    expectedResults.put(13, 1);
    expectedResults.put(14, 2);

    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(0), true));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(1), true));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(2), true));

    Assert.assertEquals(3, emittedTuples.size());
    for (Object object : emittedTuples) {
      Integer tuple = (Integer) object;
      Assert.assertEquals((int) expectedResults.get(tuple), inverseMapFctn(tuple));
    }

    Assert.assertEquals(ackedTuples, 3);
  }


//  @Test
//  public void testMapOperatorWithoutAcking() {
//    LOG.info(">>>> testing testMapOperatorWithoutAcking");
//    ackedTuples = 0;
//    config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATMOST_ONCE);
//    MapOperator<Integer, Integer> mapOperator = getMapOperator(false);
//
//    HashMap<Integer, Integer> expectedResults = new HashMap<>();
//    expectedResults.put(12, 0);
//    expectedResults.put(13, 1);
//    expectedResults.put(14, 2);
//
//    TopologyAPI.StreamId componentStreamId
//        = TopologyAPI.StreamId.newBuilder()
//        .setComponentName("sourceComponent").setId("default").build();
//
//    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(0), false));
//    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(1), false));
//    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(2), false));
//
//    Assert.assertEquals(3, emittedTuples.size());
//    for (Object object : emittedTuples) {
//      Integer tuple = (Integer) object;
//      Assert.assertEquals((int) expectedResults.get(tuple), inverseMapFctn(tuple));
//    }
//
//    Assert.assertEquals(0, ackedTuples);
//  }

  private int inverseMapFctn(int val) {
    return val - 12;
  }

  private MapOperator<Integer, Integer> getMapOperator(boolean enableAcking) {

    if (enableAcking) {
      config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    } else {
      config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATMOST_ONCE);
    }

    MapOperator<Integer, Integer> mapOperator = new MapOperator<>(x -> x + 12);

    mapOperator.prepare(config, PowerMockito.mock(TopologyContext.class),
        new OutputCollector(new IOutputCollector() {

          @Override public void reportError(Throwable error) {
          }

          @Override
          public List<Integer> emit(String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
            emittedTuples.addAll(tuple);
            LOG.info(">>>> this.emia...");
            return null;
          }

          @Override
          public void emitDirect(int taskId, String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
          }

          @Override public void ack(Tuple input) {
            ackedTuples++;
            LOG.info(">>>> acking tuple: " + input);
          }

          @Override public void fail(Tuple input) {
            LOG.info(">>>> failing tuple: " + input);
          }
        }));

    return mapOperator;
  }

  private Tuple getTuple(TopologyAPI.StreamId streamId, final Fields fields, Values values,
      boolean enableAcking) {

    TopologyContext topologyContext = getContext(fields, enableAcking);
    return  new TupleImpl(topologyContext, streamId, 0,
        null, values, 1) {
      @Override
      public TopologyAPI.StreamId getSourceGlobalStreamId() {
        return TopologyAPI.StreamId.newBuilder().setComponentName("sourceComponent")
            .setId("default").build();
      }
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TopologyContext getContext(final Fields fields, boolean enableAcking) {
    TopologyBuilder builder = new TopologyBuilder();
    if (enableAcking) {
      config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    } else {
      config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATMOST_ONCE);
    }
    LOG.info(">>>> RELIABILITY MODE: " + config.get("topology.reliability.mode"));
    return new TopologyContextImpl(config,
        builder.createTopology()
            .setConfig(config)
            .setName("test")
            .setState(TopologyAPI.TopologyState.RUNNING)
            .getTopology(),
        new HashMap(), 1, null) {
      @Override
      public Fields getComponentOutputFields(
          String componentId, String streamId) {
        return fields;
      }
    };
  }

}
