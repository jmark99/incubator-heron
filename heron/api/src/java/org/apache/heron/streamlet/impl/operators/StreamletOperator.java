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

import java.util.Random;
import java.util.logging.Logger;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.streamlet.IStreamletRichOperator;

/**
 * The Bolt interface that other operators of the streamlet packages extend.
 * The only common stuff amongst all of them is the output streams
 */
public abstract class StreamletOperator<R, T>
    extends BaseRichBolt
    implements IStreamletRichOperator<R, T> {
  private static final long serialVersionUID = 8524238140745238942L;
  static final String OUTPUT_FIELD_NAME = "output";
  static final String MSGID_FIELD_NAME = "msgId";

  private static final Logger LOG = Logger.getLogger(StreamletOperator.class.getName());

  private Random rand = new Random(System.currentTimeMillis());

  /**
   * The operators implementing streamlet functionality have some properties.
   * 1. They all output only one stream
   * 2. They should be able to consume each other's output
   * This imply that the output stream should be named same for all of them.
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    LOG.info(">>>> StreamletOperator:declareOutputFields");
    outputFieldsDeclarer.declare(new Fields(OUTPUT_FIELD_NAME /*, MSGID_FIELD_NAME*/));
  }

  // for test dev purposes. Remove before release
  public boolean dropMessage(int rate) {
    if (rand.nextInt(100) < rate) {
      return true;
    }
    return false;
  }
}
