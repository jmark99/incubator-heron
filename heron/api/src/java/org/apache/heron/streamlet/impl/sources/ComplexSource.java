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
package org.apache.heron.streamlet.impl.sources;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.ContextImpl;
import org.apache.heron.streamlet.impl.utils.StreamletUtils;

import static org.apache.heron.api.Config.TOPOLOGY_RELIABILITY_MODE;
import static org.apache.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;

/**
 * SupplierSource is a way to wrap a supplier function inside a Heron Spout.
 * The SupplierSource just calls the get method of the supplied function
 * to generate the next tuple.
 */
public class ComplexSource<R> extends StreamletSource {

  private static final long serialVersionUID = -5086763670301450007L;
  private static final Logger LOG = Logger.getLogger(ComplexSource.class.getName());
  private Source<R> generator;
  private State<Serializable, Serializable> state;

  private Map<String, R> cache = new HashMap<>();
  private boolean ackEnabled = false;
  private String msgId = null;

  public ComplexSource(Source<R> generator) {
    LOG.info(">>>> using ComplexSource...");
    this.generator = generator;
    msgId = getId();
  }

  @Override
  public void initState(State<Serializable, Serializable> startupState) {
    this.state = startupState;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map<String, Object> map, TopologyContext topologyContext,
                   SpoutOutputCollector outputCollector) {
    super.open(map, topologyContext, outputCollector);
    Context context = new ContextImpl(topologyContext, map, state);
    generator.setup(context);
    ackEnabled = isAckingEnabled(map, topologyContext);
  }

  private boolean isAckingEnabled(Map map, TopologyContext topologyContext) {
    ContextImpl context = new ContextImpl(topologyContext, map, null);
    return context.getConfig().get(TOPOLOGY_RELIABILITY_MODE).equals(ATLEAST_ONCE.toString());
  }

  @Override
  public void nextTuple() {
    Collection<R> tuples = generator.get();
    msgId = null;
    if (tuples != null) {
      for (R tuple : tuples) {
        if (ackEnabled) {
          msgId = getId();
          cache.put(msgId, tuple);
        }
        collector.emit(new Values(tuple), msgId);
        LOG.info(">>>> EMIT " + new Values(tuple, msgId));
      }
    }
  }

  @Override public void ack(Object mid) {
    if (ackEnabled) {
      R data = cache.remove(mid);
      LOG.info(">>>> ACKED [" + data + ", " + mid + "]");
    }
  }

  @Override public void fail(Object mid) {
    if (ackEnabled) {
      Values values = new Values(cache.get(mid));
      collector.emit(values, mid);
      LOG.info(">>>> RE-EMIT  [" + values.get(0) + ", " + mid + "]");
    }
  }

  private String getId() {
    return getUUID();
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }
}
