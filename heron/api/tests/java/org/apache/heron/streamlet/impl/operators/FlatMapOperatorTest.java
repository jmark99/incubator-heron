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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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

public class FlatMapOperatorTest {

  private List<Object> emittedTuples;
  private static final Logger LOG = Logger.getLogger(FlatMapOperator.class.getName());

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test
  public void testFlatMapOperator() {
    LOG.info("testing flatMapOperator");
    FlatMapOperator<String, String> flatMapOperator = getFlatMapOperator();

    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    String[] sentence = new String[]{"This is a sentence"};
    flatMapOperator.execute(getTuple(componentStreamId, new Fields("output"),
        new Values((Object) sentence)));

    Set<String> words = new HashSet<String>();
    words.add("This");
    words.add("is");
    words.add("a");
    words.add("sentence");
    Assert.assertEquals(4, emittedTuples.size());
    for (Object object : emittedTuples) {
      String tuple = (String) object;
      Assert.assertTrue(words.contains(tuple));
      words.remove(tuple);
    }
    Assert.assertTrue(words.size() == 0);
  }

  private FlatMapOperator<String, String> getFlatMapOperator() {

    FlatMapOperator<String, String> flatMapOperator
        = new FlatMapOperator<>((String line) -> Arrays.asList(line.split("\\s+")));

    flatMapOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
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

    return flatMapOperator;
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
