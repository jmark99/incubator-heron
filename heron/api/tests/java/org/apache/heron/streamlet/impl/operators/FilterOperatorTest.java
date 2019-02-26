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
import java.util.concurrent.ThreadLocalRandom;
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

public class FilterOperatorTest {

  private List<Object> emittedTuples;
  private static final Logger LOG = Logger.getLogger(FilterOperatorTest.class.getName());

  private int[] inputVals = new int[50];
  private int filterCnt = 0;

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
    int val;
    for (int i = 0; i < 50; i++) {
      val = ThreadLocalRandom.current().nextInt(-30, 30);
      if (val < 0) {
        filterCnt = filterCnt + 1;
      }
      inputVals[i] = val;
    }
  }

  @Test
  public void testFilterOperator() {
    LOG.info("testing filterOperator");
    FilterOperator<Integer> filterOperator  = getFilterOperator();

    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    for (int i = 0; i < 50; i++) {
      filterOperator.execute(getTuple(componentStreamId, new Fields("output"),
          new Values(inputVals[i])));
    }

    Assert.assertEquals(filterCnt, emittedTuples.size());
    for (Object object : emittedTuples) {
      Integer tuple = (Integer) object;
      Assert.assertTrue(tuple < 0);
    }
  }

  private FilterOperator<Integer> getFilterOperator() {

    FilterOperator<Integer> filterOperator = new FilterOperator<>(x -> x < 0);

    filterOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
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

    return filterOperator;

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
