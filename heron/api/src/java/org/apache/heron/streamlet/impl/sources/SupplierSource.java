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

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.SerializableSupplier;

import static org.apache.heron.api.Config.TOPOLOGY_RELIABILITY_MODE;
import static org.apache.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE;

/**
 * SupplierSource is a way to wrap a supplier function inside a Heron Spout.
 * The SupplierSource just calls the get method of the supplied function
 * to generate the next tuple.
 */
public class SupplierSource<R> extends StreamletSource {

  private static final long serialVersionUID = 6476611751545430216L;
  private static final Logger LOG = Logger.getLogger(SupplierSource.class.getName());

  private SerializableSupplier<R> supplier;
  private SpoutOutputCollector collector;

  public SupplierSource(SerializableSupplier<R> supplier) {
    this.supplier = supplier;
  }

  @SuppressWarnings("rawtypes") @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    collector = outputCollector;
    ackingEnabled = map.get(TOPOLOGY_RELIABILITY_MODE).equals(ATLEAST_ONCE.toString());
    msgIdCache = createCache();
  }

  @Override
  public void nextTuple() {
    msgId = null;
    R data = supplier.get();
    if (ackingEnabled) {
      msgId = getUniqueMessageId();
      msgIdCache.put(msgId, data);
      collector.emit(new Values(data), msgId);
    } else {
      collector.emit(new Values(data));
    }
    LOG.info("Emitting: [" + msgId + "]");
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
      if (values != null) {
        collector.emit(values, mid);
        LOG.info("Re-emit:  [" + mid + "]");
      } else {
        LOG.warning("Failed to retrieve cached value for msg: " + mid);
      }
    }
  }
}
