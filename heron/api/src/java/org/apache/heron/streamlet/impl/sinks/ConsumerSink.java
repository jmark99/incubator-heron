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

import java.util.logging.Logger;

import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.streamlet.SerializableConsumer;
import org.apache.heron.streamlet.impl.operators.StreamletOperator;

/**
 * ConsumerSink is a very simple Sink that basically invokes a user supplied
 * consume function for every tuple.
 */
public class ConsumerSink<R> extends StreamletOperator<R, R> {

  private static final long serialVersionUID = 8716140142187667638L;
  private static final Logger LOG = Logger.getLogger(ConsumerSink.class.getName());
  private SerializableConsumer<R> consumer;

  public ConsumerSink(SerializableConsumer<R> consumer) {
    LOG.info(">>>> using ConsumerSink");
    this.consumer = consumer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    LOG.info(">>> ConsumerSink:execute processing tuple: " + tuple.toString());
    R obj = (R) tuple.getValue(0);

    if (dropMessage(10)) {
      LOG.info(">>> ConsumerSink dropped msg: " + obj);
      return;
    }
    consumer.accept(obj);
    collector.ack(tuple);
    LOG.info(">>>  ConsumerSink sent ack...for " + tuple.toString()); //for " + tuple.toString());
  }
}
