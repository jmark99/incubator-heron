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
import java.util.logging.Logger;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Fields;

/**
 * StreamletSource is the base class for all streamlet sources.
 * The only common stuff amongst all of them is the output streams
 */
public abstract class StreamletSource extends BaseRichSpout
    implements IStatefulComponent<Serializable, Serializable> {

  private static final long serialVersionUID = 8583965332619565343L;
  private static final Logger LOG = Logger.getLogger(StreamletSource.class.getName());

  static final String OUTPUT_FIELD_NAME = "output";
  static final String MSGID_FIELD_NAME = "msgId";

  public StreamletSource() {
    LOG.info(">>>> StreamletSource Constructor called");
  }

  @Override
  public void initState(State<Serializable, Serializable> state) { }

  @Override
  public void preSave(String checkpointId) { }

  /**
   * The sources implementing streamlet functionality have some properties.
   * 1. They all output only one stream
   * 2. All streamlet operators should be able to consume their output
   * This imply that the output stream should be named same for all of them.
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    String[] schema = {OUTPUT_FIELD_NAME /*, MSGID_FIELD_NAME*/};
    outputFieldsDeclarer.declare(new Fields(schema));
  }
}
