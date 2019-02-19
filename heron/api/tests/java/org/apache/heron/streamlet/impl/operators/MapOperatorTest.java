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

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test
  public void testMapOperator() {
    System.out.println("testing mapOperator...");
    MapOperator<Integer, Integer> mapOperator = getMapOperator();

    HashMap<Integer, Integer> expectedResults = new HashMap<>();
    expectedResults.put(12, 0);
    expectedResults.put(13, 1);
    expectedResults.put(14, 2);

    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(0)));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(1)));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values(2)));

    Assert.assertEquals(3, emittedTuples.size());
    for (Object object : emittedTuples) {
      Integer tuple = (Integer) object;
      Assert.assertEquals((int) expectedResults.get(tuple), inverseMapFctn(tuple));
    }
  }

  private int inverseMapFctn(int val) {
    return val - 12;
  }


  @Test
  public void testMapOperatorWithAcks() {
    System.out.println("testing testMapOperatorWithAcks...");
    MapOperator<String, String> mapOperator = getMapOperator2();

    HashMap<String, String> expectedResults = new HashMap<>();
    expectedResults.put("a!", "a");
    expectedResults.put("b!", "b");
    expectedResults.put("c!", "c");

    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values("a")));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values("b")));
    mapOperator.execute(getTuple(componentStreamId, new Fields("output"), new Values("c")));

    Assert.assertEquals(3, emittedTuples.size());
    for (Object object : emittedTuples) {
      String tuple = (String) object;
      Assert.assertEquals(expectedResults.get(tuple), inverseMapFctn2(tuple));
    }
  }

  private String inverseMapFctn2(String str) {
    return str.substring(0, str.length() - 1);
  }

  private MapOperator<Integer, Integer> getMapOperator() {

    MapOperator<Integer, Integer> mapOperator = new MapOperator<>(x -> x + 12);

    mapOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
        new OutputCollector(new IOutputCollector() {

          @Override public void reportError(Throwable error) {
          }

          @Override
          public List<Integer> emit(String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
            emittedTuples.addAll(tuple);
            return null;
          }

          @Override
          public void emitDirect(int taskId, String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
          }

          @Override public void ack(Tuple input) {
            System.out.println(">>> calling ack");
          }

          @Override public void fail(Tuple input) {
            System.out.println(">>> calling fail");
          }
        }));

    return mapOperator;
  }


  private MapOperator<String, String> getMapOperator2() {

    MapOperator<String, String> mapOperator = new MapOperator<>(x -> x + "!");

    mapOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
        new OutputCollector(new IOutputCollector() {

          @Override public void reportError(Throwable error) {
          }

          @Override
          public List<Integer> emit(String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
            emittedTuples.addAll(tuple);
            return null;
          }

          @Override
          public void emitDirect(int taskId, String streamId,
              Collection<Tuple> anchors, List<Object> tuple) {
          }

          @Override public void ack(Tuple input) {
          }

          @Override public void fail(Tuple input) {
          }
        }));

    return mapOperator;
  }



  private Tuple getTuple(TopologyAPI.StreamId streamId, final Fields fields, Values values) {

    TopologyContext topologyContext = getContext(fields);
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
  private TopologyContext getContext(final Fields fields) {
    TopologyBuilder builder = new TopologyBuilder();
    return new TopologyContextImpl(new Config(),
        builder.createTopology()
            .setConfig(new Config())
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
