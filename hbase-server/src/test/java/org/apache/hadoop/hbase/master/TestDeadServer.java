/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MediumTests.class)
public class TestDeadServer {
  final ServerName hostname123 = new ServerName("127.0.0.1", 123, 3L);
  final ServerName hostname123_2 = new ServerName("127.0.0.1", 123, 4L);
  final ServerName hostname1234 = new ServerName("127.0.0.2", 1234, 4L);
  final ServerName hostname12345 = new ServerName("127.0.0.2", 12345, 4L);

  @Test public void testIsDead() {
    DeadServer ds = new DeadServer();
    ds.add(hostname123);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname123);
    assertFalse(ds.areDeadServersInProgress());

    ds.add(hostname1234);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname1234);
    assertFalse(ds.areDeadServersInProgress());

    ds.add(hostname12345);
    assertTrue(ds.areDeadServersInProgress());
    ds.finish(hostname12345);
    assertFalse(ds.areDeadServersInProgress());

    // Already dead =       127.0.0.1,9090,112321
    // Coming back alive =  127.0.0.1,9090,223341

    final ServerName deadServer = new ServerName("127.0.0.1", 9090, 112321L);
    assertFalse(ds.cleanPreviousInstance(deadServer));
    ds.add(deadServer);
    assertTrue(ds.isDeadServer(deadServer));
    final ServerName deadServerHostComingAlive =
      new ServerName("127.0.0.1", 9090, 223341L);
    assertTrue(ds.cleanPreviousInstance(deadServerHostComingAlive));
    assertFalse(ds.isDeadServer(deadServer));
    assertFalse(ds.cleanPreviousInstance(deadServerHostComingAlive));
  }


  @Test
  public void testSortExtract(){
    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(mee);
    mee.setValue(1);

    DeadServer d = new DeadServer();


    d.add(hostname123);
    mee.incValue(1);
    d.add(hostname1234);
    mee.incValue(1);
    d.add(hostname12345);

    List<Pair<ServerName, Long>> copy = d.copyDeadServersSince(2L);
    Assert.assertEquals(2, copy.size());

    Assert.assertEquals(hostname1234, copy.get(0).getFirst());
    Assert.assertEquals(new Long(2L), copy.get(0).getSecond());

    Assert.assertEquals(hostname12345, copy.get(1).getFirst());
    Assert.assertEquals(new Long(3L), copy.get(1).getSecond());

    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testClean(){
    DeadServer d = new DeadServer();
    d.add(hostname123);

    d.cleanPreviousInstance(hostname12345);
    Assert.assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname1234);
    Assert.assertFalse(d.isEmpty());

    d.cleanPreviousInstance(hostname123_2);
    Assert.assertTrue(d.isEmpty());
  }

}

