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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.Config.TopologyReliabilityMode;
import org.apache.heron.api.spout.ISpoutOutputCollector;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.streamlet.SerializableSupplier;

import static org.powermock.api.mockito.PowerMockito.mock;


public class SupplierSourceTest {

  private static final Logger LOG = Logger.getLogger(SupplierSourceTest.class.getName());

  private AtomicInteger atomicInteger = new AtomicInteger(0);
  private SupplierSource source;

  private SerializableSupplier<Integer> supplier =
      (SerializableSupplier<Integer>) () -> atomicInteger.getAndIncrement();

  private Map<String, Object> confMap = new HashMap<>();
  private TopologyContext mockContext = mock(TopologyContext.class);
  private ISpoutOutputCollector myCollector;


  public SupplierSourceTest() {
    confMap.put("topology.reliability.mode", TopologyReliabilityMode.ATMOST_ONCE);
    SpoutOutputCollector mySpout = new SpoutOutputCollector(new TestCollector());
    source = new SupplierSource<>(supplier);
    source.open(confMap, mockContext, mySpout);
  }


  @Before
  public void preTestSetup() {
    source.msgIdCache.invalidateAll();
    source.tskIds = null;
  }

  @Test
  public void testAckWithAcking() {
    source.ackingEnabled = true;
    // clear all cache entries
    Assert.assertEquals(0, source.msgIdCache.size());
    // Add an 'message id' entry to cache
    source.msgIdCache.put("mid", "1");
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertEquals("1", source.msgIdCache.getIfPresent("mid"));
    source.ack("mid");
    // verify the message id entry is no longer in the cache
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNull(source.msgIdCache.getIfPresent("mid"));
  }



  @Test
  public void testAckWithoutAcking1() {
    LOG.info("*********************************");
    source.ackingEnabled = false;
    LOG.info(">>>> set isAckingEnabled to false: " + source.ackingEnabled);
    // clear all cache entries
    Assert.assertEquals(0, source.msgIdCache.size());
    // Add an 'message id' entry to cache
    source.msgIdCache.put("id1", "1");
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertEquals("1", source.msgIdCache.getIfPresent("id1"));
    source.ack("id1");
    // with no acking, the msgIdCache is not involved so size should still be 1
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertNotNull(source.msgIdCache.getIfPresent("id1"));
  }

  @Test
  public void testAckWithoutAcking2() {
    source.ackingEnabled = false;
    Assert.assertEquals(0, source.msgIdCache.size());
    source.ack("id1");
    // with no acking, the msgIdCache is not involved so size should still be 0
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNull(source.msgIdCache.getIfPresent("id1"));
  }

  @Test
  public void testFailWithAcking() {
    source.ackingEnabled = true;
    // Add an entry to the cache to be used in failure test
    source.msgIdCache.put("mid", 1423);
    Assert.assertEquals(1, source.msgIdCache.size());
    source.fail("mid");
    Assert.assertNotNull(source.tskIds);
    // cache should still retain value
    Assert.assertEquals(1, source.msgIdCache.size());
  }


  @Test
  public void testFailWithoutAcking() {
    source.ackingEnabled = false;
    // without acking, the fail method is basically a no-op. Verify that nothing is emitted
    Object mid = "mid";
    source.fail(mid);
    Assert.assertNull(source.tskIds);
  }

  @Test
  public void testNextTupleWithAcking() {
    source.ackingEnabled = true;
    source.nextTuple();
    Assert.assertEquals(1, source.msgIdCache.size());
    Assert.assertNotNull(source.tskIds);
  }

  @Test
  public void testNextTupleWithoutAcking() {
    source.ackingEnabled = false;
    source.nextTuple();
    LOG.info(">>>> taskIds: " + source.tskIds);
    // assert taskIds equal value to supplier, i.e., 0, 1, 2, etc
    Assert.assertEquals(0, source.msgIdCache.size());
    Assert.assertNotNull(source.tskIds);
  }

}
