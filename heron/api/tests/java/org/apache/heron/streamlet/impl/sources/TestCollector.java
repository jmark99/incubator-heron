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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.heron.api.spout.ISpoutOutputCollector;


public class TestCollector implements ISpoutOutputCollector {

  private static final Logger LOG = Logger.getLogger(TestCollector.class.getName());

  @Override public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    LOG.info("**** emit...");
    List<Integer> tskIds = new ArrayList<>();
    // set tskIds to value of tuple unless null
    if (tuple  == null) {
      tskIds.add(996985999);
    } else {
      tskIds.add((Integer) tuple.get(0));
    }

    LOG.info("**** TestCollector::emit - emitting " + tuple.toString());
    return tskIds;
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
    LOG.info("**** emitDirect...");
  }

  @Override public void reportError(Throwable error) {
    LOG.info("**** reportError...");
  }
}
