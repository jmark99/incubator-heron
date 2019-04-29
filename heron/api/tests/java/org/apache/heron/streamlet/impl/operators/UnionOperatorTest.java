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

public class UnionOperatorTest {

  private List<Object> emittedTuples;
  private static final Logger LOG = Logger.getLogger(UnionOperatorTest.class.getName());

  @Before public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test public void testUnionOperator() {
    UnionOperator<Integer> unionOperator = getUnionOperator();

    HashMap<Integer, Integer> expectedResults = new HashMap<>();
    expectedResults.put(0, 0);
    expectedResults.put(1, 1);
    expectedResults.put(2, 2);
    expectedResults.put(3, 3);

    TopologyAPI.StreamId componentStreamId1 = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent1").setId("default").build();

    TopologyAPI.StreamId componentStreamId2 = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent2").setId("default2").build();

    unionOperator.execute(getTuple(componentStreamId1, new Fields("output1"),
        new Values(0)));
    unionOperator.execute(getTuple(componentStreamId1, new Fields("output1"),
        new Values(2)));

    unionOperator.execute(getTuple(componentStreamId2, new Fields("output2"),
        new Values(1)));
    unionOperator.execute(getTuple(componentStreamId2, new Fields("output2"),
        new Values(3)));

    Assert.assertEquals(4, emittedTuples.size());
    for (Object object : emittedTuples) {
      Integer tuple = (Integer) object;
      Assert.assertEquals(expectedResults.get(tuple), tuple);
    }
  }

  private UnionOperator<Integer> getUnionOperator() {

    UnionOperator<Integer> unionOperator = new UnionOperator<>();

    unionOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
        new OutputCollector(new IOutputCollector() {

          @Override public void reportError(Throwable error) {
          }

          @Override public List<Integer> emit(String streamId, Collection<Tuple> anchors,
              List<Object> tuple) {
            emittedTuples.addAll(tuple);
            return null;
          }

          @Override
          public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors,
              List<Object> tuple) {
          }

          @Override public void ack(Tuple input) {
          }

          @Override public void fail(Tuple input) {
          }
        }));

    return unionOperator;
  }

  private Tuple getTuple(TopologyAPI.StreamId streamId, final Fields fields, Values values) {

    TopologyContext topologyContext = getContext(fields);
    return new TupleImpl(topologyContext, streamId, 0, null, values, 1) {
      @Override public TopologyAPI.StreamId getSourceGlobalStreamId() {
        return TopologyAPI.StreamId.newBuilder().setComponentName("sourceComponent")
            .setId("default").build();
      }
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TopologyContext getContext(final Fields fields) {
    TopologyBuilder builder = new TopologyBuilder();
    return new TopologyContextImpl(new Config(),
        builder.createTopology().setConfig(new Config()).setName("test")
            .setState(TopologyAPI.TopologyState.RUNNING).getTopology(), new HashMap(), 1, null) {
      @Override public Fields getComponentOutputFields(String componentId, String streamId) {
        return fields;
      }
    };
  }
}
