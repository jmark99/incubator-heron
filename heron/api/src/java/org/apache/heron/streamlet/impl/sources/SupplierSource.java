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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.SerializableSupplier;
import org.apache.heron.streamlet.impl.ContextImpl;

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

  private Map<String, R> cache = new HashMap<>();
  private boolean ackEnabled = false;
  private String msgId = null;

  public SupplierSource(SerializableSupplier<R> supplier) {
    LOG.info(">>>> Using SupplierSource...");
    this.supplier = supplier;
    msgId = getId();
  }

  @SuppressWarnings("rawtypes") @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    collector = outputCollector;
    ackEnabled = isAckingEnabled(map, topologyContext);
  }

  private boolean isAckingEnabled(Map map, TopologyContext topologyContext) {
    ContextImpl context = new ContextImpl(topologyContext, map, null);
    return context.getConfig().get(TOPOLOGY_RELIABILITY_MODE).equals(ATLEAST_ONCE.toString());
  }

  @Override public void nextTuple() {
    R r = supplier.get();
    if (!ackEnabled) {
      msgId = null;
    } else {
      msgId = getId();
      cache.put(msgId, r);
    }
    collector.emit(new Values(r), msgId);
    LOG.info(">>>> SUPPLIERSOURCE::nextTuple -> EMIT " + new Values(r, msgId));
  }

  @Override public void ack(Object mid) {
    if (ackEnabled) {
      R data = cache.remove(mid);
      //R data = cache.get(mid);
      LOG.info(">>>> SUPPLIERSOURCE::ack --------> ACKED [" + data + ", " + mid + "]");
    }
  }

  @Override public void fail(Object mid) {
    if (ackEnabled) {
      Values values = new Values(cache.get(mid));
      collector.emit(values, mid);
      LOG.info(">>>> SUPPLIERSOURCE::failed -------> RE-EMIT  [" + values + ", " + mid + "]");
    }
  }

  private static AtomicLong idCounter = new AtomicLong();

  private String getId() {
    return "MID" + getUUID();
  }

  private String getUUID() {
    return UUID.randomUUID().toString();
  }
}
