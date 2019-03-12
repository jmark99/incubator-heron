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
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.ContextImpl;

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

  public ComplexSource(Source<R> generator) {
    this.generator = generator;
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
    ackingEnabled = map.get(TOPOLOGY_RELIABILITY_MODE).equals(ATLEAST_ONCE.toString());
    msgIdCache = createCache();
  }

  @Override
  public void nextTuple() {
    Collection<R> tuples = generator.get();
    msgId = null;
    if (tuples != null) {
      for (R tuple : tuples) {
        if (ackingEnabled) {
          msgId = getUniqueMessageId();
          msgIdCache.put(msgId, tuple);
          collector.emit(new Values(tuple), msgId);
        } else {
          collector.emit(new Values(tuple));
        }
        LOG.info("Emitting: [" + msgId + "]");
      }
    }
  }

  @Override
  public void ack(Object mid) {
    if (ackingEnabled) {
      msgIdCache.invalidate(mid);
      LOG.info("Acked:    [" + mid + "]");
    }
  }

  @Override
  public void fail(Object mid) {
    if (ackingEnabled) {
      Values values = new Values(msgIdCache.getIfPresent(mid));
      if (values.get(0) != null) {
        collector.emit(values, mid);
        LOG.info("Re-emit:  [" + mid + "]");
      } else {
        // will not re-emit since value cannot be retrieved.
        LOG.severe("Failed to retrieve cached value for msg: " + mid);
      }
    }
  }
}
