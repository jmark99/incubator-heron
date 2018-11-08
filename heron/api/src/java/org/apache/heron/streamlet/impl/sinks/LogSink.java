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


package org.apache.heron.streamlet.impl.sinks;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.streamlet.impl.operators.StreamletOperator;

/**
 * LogSink is a very simple Bolt that implements the log functionality.
 * It basically logs every tuple.
 */
public class LogSink<R> extends StreamletOperator<R, R> {
  private static final long serialVersionUID = -6392422646613189818L;
  private static final Logger LOG = Logger.getLogger(LogSink.class.getName());
  private OutputCollector collector;

  public LogSink() {
    LOG.info(">>>> Using LogSink()");
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);

    if (dropMessage(10)) {
      LOG.info(">>> LogSink dropped msg: " + obj);
      return;
    }

    LOG.info(">>>  " + String.valueOf(obj));
    collector.ack(tuple);
    LOG.info(">>>  LogSink sent ack for " + obj);
  }
}
