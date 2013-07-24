/**
 *
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

import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_wait_for_zk_delete;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_final_transition_failed;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_preempt_task;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_done;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_err;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_wkr_task_resigned;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.exceptions.RegionInRecoveryException;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestDistributedLogSplitting {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);

    // test ThreeRSAbort fails under hadoop2 (2.0.2-alpha) if shortcircuit-read (scr) is on. this
    // turns it off for this test.  TODO: Figure out why scr breaks recovery. 
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

  }

  // Start a cluster with 2 masters and 6 regionservers
  final int NUM_MASTERS = 2;
  final int NUM_RS = 6;

  MiniHBaseCluster cluster;
  HMaster master;
  Configuration conf;
  HBaseTestingUtility TEST_UTIL;

  private void startCluster(int num_rs) throws Exception{
    conf = HBaseConfiguration.create();
    startCluster(num_rs, conf);
  }

  private void startCluster(int num_rs, Configuration inConf) throws Exception {
    SplitLogCounters.resetCounters();
    LOG.info("Starting cluster");
    this.conf = inConf;
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, num_rs);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
    while (cluster.getLiveRegionServerThreads().size() < num_rs) {
      Threads.sleep(1);
    }
  }

  @After
  public void after() throws Exception {
    for (MasterThread mt : TEST_UTIL.getHBaseCluster().getLiveMasterThreads()) {
      mt.getMaster().abort("closing...", new Exception("Trace info"));
    }

    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout=300000)
  public void testRecoveredEdits() throws Exception {
    LOG.info("testRecoveredEdits");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    startCluster(NUM_RS, curConf);

    final int NUM_LOG_LINES = 1000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();

    Path rootdir = FSUtils.getRootDir(conf);

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);
    TableName table = TableName.valueOf("table");
    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      if (regions.size() != 0) break;
    }
    final Path logDir = new Path(rootdir, HLogUtil.getHLogDirectoryName(hrs
        .getServerName().toString()));

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.getTableName().getNamespaceAsString()
          .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    slm.splitLogDistributed(logDir);

    int count = 0;
    for (HRegionInfo hri : regions) {

      Path tdir = FSUtils.getTableDir(rootdir, table);
      @SuppressWarnings("deprecation")
      Path editsdir =
        HLogUtil.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      FileStatus[] files = fs.listStatus(editsdir);
      assertEquals(1, files.length);
      int c = countHLog(files[0].getPath(), fs, conf);
      count += c;
      LOG.info(c + " edits in " + files[0].getPath());
    }
    assertEquals(NUM_LOG_LINES, count);
  }

  @Test(timeout = 300000)
  public void testLogReplayWithNonMetaRSDown() throws Exception {
    LOG.info("testLogReplayWithNonMetaRSDown");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setLong("hbase.regionserver.hlog.blocksize", 100*1024);
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    // wait for abort completes
    this.abortRSAndVerifyRecovery(hrs, ht, zkw, NUM_REGIONS_TO_CREATE, NUM_LOG_LINES);
    ht.close();
  }

  @Test(timeout = 300000)
  public void testLogReplayWithMetaRSDown() throws Exception {
    LOG.info("testRecoveredEditsReplayWithMetaRSDown");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (!isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    this.abortRSAndVerifyRecovery(hrs, ht, zkw, NUM_REGIONS_TO_CREATE, NUM_LOG_LINES);
    ht.close();
  }

  private void abortRSAndVerifyRecovery(HRegionServer hrs, HTable ht, final ZooKeeperWatcher zkw,
      final int numRegions, final int numofLines) throws Exception {

    abortRSAndWaitForRecovery(hrs, zkw, numRegions);
    assertEquals(numofLines, TEST_UTIL.countRows(ht));
  }

  private void abortRSAndWaitForRecovery(HRegionServer hrs, final ZooKeeperWatcher zkw,
      final int numRegions) throws Exception {
    final MiniHBaseCluster tmpCluster = this.cluster;

    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (tmpCluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(tmpCluster).size() >= (numRegions + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });
  }

  @Test(timeout = 300000)
  public void testMasterStartsUpWithLogSplittingWork() throws Exception {
    LOG.info("testMasterStartsUpWithLogSplittingWork");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    curConf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(NUM_RS, curConf);

    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    // abort master
    abortMaster(cluster);

    // abort RS
    int numRS = cluster.getLiveRegionServerThreads().size();
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    Thread.sleep(2000);
    LOG.info("Current Open Regions:" + getAllOnlineRegions(cluster).size());
    
    startMasterAndWaitUntilLogSplit(cluster);
    
    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(cluster).size() >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    LOG.info("Current Open Regions After Master Node Starts Up:"
        + getAllOnlineRegions(cluster).size());

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    ht.close();
  }
  
  @Test(timeout = 300000)
  public void testMasterStartsUpWithLogReplayWork() throws Exception {
    LOG.info("testMasterStartsUpWithLogReplayWork");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    curConf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, NUM_RS - 1);
    startCluster(NUM_RS, curConf);

    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    // abort master
    abortMaster(cluster);

    // abort RS
    int numRS = cluster.getLiveRegionServerThreads().size();
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for the RS dies
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    Thread.sleep(2000);
    LOG.info("Current Open Regions:" + getAllOnlineRegions(cluster).size());
    
    startMasterAndWaitUntilLogSplit(cluster);
    
    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    LOG.info("Current Open Regions After Master Node Starts Up:"
        + getAllOnlineRegions(cluster).size());

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));

    ht.close();
  }
  
  
  @Test(timeout = 300000)
  public void testLogReplayTwoSequentialRSDown() throws Exception {
    LOG.info("testRecoveredEditsReplayTwoSequentialRSDown");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs1 = rsts.get(0).getRegionServer();
    regions = ProtobufUtil.getOnlineRegions(hrs1);

    makeHLog(hrs1.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);

    // abort RS1
    LOG.info("Aborting region server: " + hrs1.getServerName());
    hrs1.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(cluster).size() >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // sleep a little bit in order to interrupt recovering in the middle
    Thread.sleep(300);
    // abort second region server
    rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs2 = rsts.get(0).getRegionServer();
    LOG.info("Aborting one more region server: " + hrs2.getServerName());
    hrs2.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 2));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(cluster).size() >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));
    ht.close();
  }

  @Test(timeout = 300000)
  public void testMarkRegionsRecoveringInZK() throws Exception {
    LOG.info("testMarkRegionsRecoveringInZK");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    master.balanceSwitch(false);
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = master.getZooKeeperWatcher();
    HTable ht = installTable(zkw, "table", "family", 40);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;

    Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
    HRegionInfo region = null;
    HRegionServer hrs = null;
    ServerName firstFailedServer = null;
    ServerName secondFailedServer = null;
    for (int i = 0; i < NUM_RS; i++) {
      hrs = rsts.get(i).getRegionServer();
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs);
      if (regions.isEmpty()) continue;
      region = regions.get(0);
      regionSet.add(region);
      firstFailedServer = hrs.getServerName();
      secondFailedServer = rsts.get((i + 1) % NUM_RS).getRegionServer().getServerName();
      break;
    }

    slm.markRegionsRecoveringInZK(firstFailedServer, regionSet);
    slm.markRegionsRecoveringInZK(secondFailedServer, regionSet);

    List<String> recoveringRegions = ZKUtil.listChildrenNoWatch(zkw,
      ZKUtil.joinZNode(zkw.recoveringRegionsZNode, region.getEncodedName()));

    assertEquals(recoveringRegions.size(), 2);

    // wait for splitLogWorker to mark them up because there is no WAL files recorded in ZK
    final HRegionServer tmphrs = hrs;
    TEST_UTIL.waitFor(60000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (tmphrs.getRecoveringRegions().size() == 0);
      }
    });
    ht.close();
  }

  @Test(timeout = 300000)
  public void testReplayCmd() throws Exception {
    LOG.info("testReplayCmd");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    this.prepareData(ht, Bytes.toBytes("family"), Bytes.toBytes("c1"));
    String originalCheckSum = TEST_UTIL.checksumRows(ht);
    
    // abort RA and trigger replay
    abortRSAndWaitForRecovery(hrs, zkw, NUM_REGIONS_TO_CREATE);

    assertEquals("Data should remain after reopening of regions", originalCheckSum,
      TEST_UTIL.checksumRows(ht));

    ht.close();
  }

  @Test(timeout = 300000)
  public void testLogReplayForDisablingTable() throws Exception {
    LOG.info("testLogReplayWithNonMetaRSDown");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 1000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable disablingHT = installTable(zkw, "disableTable", "family", NUM_REGIONS_TO_CREATE);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE, NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "disableTable", "family", NUM_LOG_LINES, 100, false);
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);
    
    LOG.info("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(Bytes.toBytes("disableTable"));
    
    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");

    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });

    // wait for regions come online
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(cluster).size() >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    int count = 0;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path rootdir = FSUtils.getRootDir(conf);
    Path tdir = FSUtils.getTableDir(rootdir, TableName.valueOf("disableTable"));
    for (HRegionInfo hri : regions) {
      @SuppressWarnings("deprecation")
      Path editsdir =
        HLogUtil.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      LOG.debug("checking edits dir " + editsdir);
      if(!fs.exists(editsdir)) continue;
      FileStatus[] files = fs.listStatus(editsdir);
      if(files != null) {
        for(FileStatus file : files) {
          int c = countHLog(file.getPath(), fs, conf);
          count += c;
          LOG.info(c + " edits in " + file.getPath());
        }
      }
    }

    LOG.info("Verify edits in recovered.edits files");
    assertEquals(NUM_LOG_LINES, count);
    LOG.info("Verify replayed edits");
    assertEquals(NUM_LOG_LINES, TEST_UTIL.countRows(ht));
    
    // clean up
    for (HRegionInfo hri : regions) {
      @SuppressWarnings("deprecation")
      Path editsdir =
        HLogUtil.getRegionDirRecoveredEditsDir(HRegion.getRegionDir(tdir, hri.getEncodedName()));
      fs.delete(editsdir, true);
    }
    disablingHT.close();
    ht.close();
  }

  @Test(timeout = 300000)
  public void testDisallowWritesInRecovering() throws Exception {
    LOG.info("testDisallowWritesInRecovering");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    curConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    curConf.setBoolean(HConstants.DISALLOW_WRITES_IN_RECOVERING, true);
    startCluster(NUM_RS, curConf);
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_LOG_LINES = 20000;
    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "table-creation", null);
    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);

    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Iterator<HRegionInfo> it = regions.iterator();
    while (it.hasNext()) {
      HRegionInfo region = it.next();
      if (region.isMetaTable()) {
        it.remove();
      }
    }
    makeHLog(hrs.getWAL(), regions, "table", "family", NUM_LOG_LINES, 100);
    
    // abort RS
    LOG.info("Aborting region server: " + hrs.getServerName());
    hrs.abort("testing");
    
    // wait for abort completes
    TEST_UTIL.waitFor(120000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (cluster.getLiveRegionServerThreads().size() <= (NUM_RS - 1));
      }
    });
    
    // wait for regions come online
    TEST_UTIL.waitFor(180000, 100, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (getAllOnlineRegions(cluster).size() >= (NUM_REGIONS_TO_CREATE + 1));
      }
    });

    try {
      HRegionInfo region = regions.get(0);
      byte[] key = region.getStartKey();
      if (key == null || key.length == 0) {
        key = new byte[] { 0, 0, 0, 0, 1 };
      }
      ht.setAutoFlush(true);
      Put put = new Put(key);
      put.add(Bytes.toBytes("family"), Bytes.toBytes("c1"), new byte[]{'b'});
      ht.put(put);
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof RetriesExhaustedWithDetailsException);
      RetriesExhaustedWithDetailsException re = (RetriesExhaustedWithDetailsException) ioe;
      Assert.assertTrue(re.getCause(0) instanceof RegionInRecoveryException);
    }

    ht.close();
  }

  /**
   * The original intention of this test was to force an abort of a region
   * server and to make sure that the failure path in the region servers is
   * properly evaluated. But it is difficult to ensure that the region server
   * doesn't finish the log splitting before it aborts. Also now, there is
   * this code path where the master will preempt the region server when master
   * detects that the region server has aborted.
   * @throws Exception
   */
  @Test (timeout=300000)
  public void testWorkerAbort() throws Exception {
    LOG.info("testWorkerAbort");
    startCluster(3);
    final int NUM_LOG_LINES = 10000;
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    final List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    HRegionServer hrs = rsts.get(0).getRegionServer();
    Path rootdir = FSUtils.getRootDir(conf);
    final Path logDir = new Path(rootdir,
        HLogUtil.getHLogDirectoryName(hrs.getServerName().toString()));

    installTable(new ZooKeeperWatcher(conf, "table-creation", null),
        "table", "family", 40);

    makeHLog(hrs.getWAL(), ProtobufUtil.getOnlineRegions(hrs), "table", "family", NUM_LOG_LINES,
      100);

    new Thread() {
      public void run() {
        waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
        for (RegionServerThread rst : rsts) {
          rst.getRegionServer().abort("testing");
          break;
        }
      }
    }.start();
    // slm.splitLogDistributed(logDir);
    FileStatus[] logfiles = fs.listStatus(logDir);
    TaskBatch batch = new TaskBatch();
    slm.enqueueSplitTask(logfiles[0].getPath().toString(), batch);
    //waitForCounter but for one of the 2 counters
    long curt = System.currentTimeMillis();
    long waitTime = 80000;
    long endt = curt + waitTime;
    while (curt < endt) {
      if ((tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
          tot_wkr_final_transition_failed.get() + tot_wkr_task_done.get() +
          tot_wkr_preempt_task.get()) == 0) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(1, (tot_wkr_task_resigned.get() + tot_wkr_task_err.get() +
            tot_wkr_final_transition_failed.get() + tot_wkr_task_done.get() +
            tot_wkr_preempt_task.get()));
        return;
      }
    }
    fail("none of the following counters went up in " + waitTime +
        " milliseconds - " +
        "tot_wkr_task_resigned, tot_wkr_task_err, " +
        "tot_wkr_final_transition_failed, tot_wkr_task_done, " +
        "tot_wkr_preempt_task");
  }

  @Test (timeout=300000)
  public void testThreeRSAbort() throws Exception {
    LOG.info("testThreeRSAbort");
    final int NUM_REGIONS_TO_CREATE = 40;
    final int NUM_ROWS_PER_REGION = 100;

    startCluster(NUM_RS); // NUM_RS=6.

    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf,
        "distributed log splitting test", null);

    HTable ht = installTable(zkw, "table", "family", NUM_REGIONS_TO_CREATE);
    populateDataInTable(NUM_ROWS_PER_REGION, "family");


    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());
    rsts.get(0).getRegionServer().abort("testing");
    rsts.get(1).getRegionServer().abort("testing");
    rsts.get(2).getRegionServer().abort("testing");

    long start = EnvironmentEdgeManager.currentTimeMillis();
    while (cluster.getLiveRegionServerThreads().size() > (NUM_RS - 3)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue(false);
      }
      Thread.sleep(200);
    }

    start = EnvironmentEdgeManager.currentTimeMillis();
    while (getAllOnlineRegions(cluster).size() < (NUM_REGIONS_TO_CREATE + 1)) {
      if (EnvironmentEdgeManager.currentTimeMillis() - start > 60000) {
        assertTrue("Timedout", false);
      }
      Thread.sleep(200);
    }

    // wait for all regions are fully recovered
    TEST_UTIL.waitFor(180000, 200, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> recoveringRegions = zkw.getRecoverableZooKeeper().getChildren(
          zkw.recoveringRegionsZNode, false);
        return (recoveringRegions != null && recoveringRegions.size() == 0);
      }
    });

    assertEquals(NUM_REGIONS_TO_CREATE * NUM_ROWS_PER_REGION,
        TEST_UTIL.countRows(ht));
    ht.close();
  }



  @Test(timeout=30000)
  public void testDelayedDeleteOnFailure() throws Exception {
    LOG.info("testDelayedDeleteOnFailure");
    startCluster(1);
    final SplitLogManager slm = master.getMasterFileSystem().splitLogManager;
    final FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final Path logDir = new Path(FSUtils.getRootDir(conf), "x");
    fs.mkdirs(logDir);
    ExecutorService executor = null;
    try {
      final Path corruptedLogFile = new Path(logDir, "x");
      FSDataOutputStream out;
      out = fs.create(corruptedLogFile);
      out.write(0);
      out.write(Bytes.toBytes("corrupted bytes"));
      out.close();
      slm.ignoreZKDeleteForTesting = true;
      executor = Executors.newSingleThreadExecutor();
      Runnable runnable = new Runnable() {
       @Override
       public void run() {
          try {
            // since the logDir is a fake, corrupted one, so the split log worker
            // will finish it quickly with error, and this call will fail and throw
            // an IOException.
            slm.splitLogDistributed(logDir);
          } catch (IOException ioe) {
            try {
              assertTrue(fs.exists(corruptedLogFile));
              // this call will block waiting for the task to be removed from the
              // tasks map which is not going to happen since ignoreZKDeleteForTesting
              // is set to true, until it is interrupted.
              slm.splitLogDistributed(logDir);
            } catch (IOException e) {
              assertTrue(Thread.currentThread().isInterrupted());
              return;
            }
            fail("did not get the expected IOException from the 2nd call");
          }
          fail("did not get the expected IOException from the 1st call");
        }
      };
      Future<?> result = executor.submit(runnable);
      try {
        result.get(2000, TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        // it is ok, expected.
      }
      waitForCounter(tot_mgr_wait_for_zk_delete, 0, 1, 10000);
      executor.shutdownNow();
      executor = null;

      // make sure the runnable is finished with no exception thrown.
      result.get();
    } finally {
      if (executor != null) {
        // interrupt the thread in case the test fails in the middle.
        // it has no effect if the thread is already terminated.
        executor.shutdownNow();
      }
      fs.delete(logDir, true);
    }
  }

  @Test(timeout = 300000)
  public void testMetaRecoveryInZK() throws Exception {
    LOG.info("testMetaRecoveryInZK");
    Configuration curConf = HBaseConfiguration.create();
    curConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);
    startCluster(NUM_RS, curConf);

    // turn off load balancing to prevent regions from moving around otherwise
    // they will consume recovered.edits
    master.balanceSwitch(false);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    final ZooKeeperWatcher zkw = new ZooKeeperWatcher(curConf, "table-creation", null);
    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();

    installTable(zkw, "table", "family", 40);
    List<HRegionInfo> regions = null;
    HRegionServer hrs = null;
    for (int i = 0; i < NUM_RS; i++) {
      boolean isCarryingMeta = false;
      hrs = rsts.get(i).getRegionServer();
      regions = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo region : regions) {
        if (region.isMetaRegion()) {
          isCarryingMeta = true;
          break;
        }
      }
      if (!isCarryingMeta) {
        continue;
      }
      break;
    }

    LOG.info("#regions = " + regions.size());
    Set<HRegionInfo> tmpRegions = new HashSet<HRegionInfo>();
    tmpRegions.add(HRegionInfo.FIRST_META_REGIONINFO);
    master.getMasterFileSystem().prepareMetaLogReplay(hrs.getServerName(), tmpRegions);
    Set<ServerName> failedServers = new HashSet<ServerName>();
    failedServers.add(hrs.getServerName());
    master.getMasterFileSystem().prepareLogReplay(failedServers);
    boolean isMetaRegionInRecovery = false;
    List<String> recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);
    for (String curEncodedRegionName : recoveringRegions) {
      if (curEncodedRegionName.equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        isMetaRegionInRecovery = true;
        break;
      }
    }
    assertTrue(isMetaRegionInRecovery);

    master.getMasterFileSystem().splitMetaLog(hrs.getServerName());
    
    isMetaRegionInRecovery = false;
    recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);
    for (String curEncodedRegionName : recoveringRegions) {
      if (curEncodedRegionName.equals(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName())) {
        isMetaRegionInRecovery = true;
        break;
      }
    }
    // meta region should be recovered
    assertFalse(isMetaRegionInRecovery);
  }

  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs) throws Exception {
    return installTable(zkw, tname, fname, nrs, 0);
  }

  HTable installTable(ZooKeeperWatcher zkw, String tname, String fname, int nrs, 
      int existingRegions) throws Exception {
    // Create a table with regions
    byte [] table = Bytes.toBytes(tname);
    byte [] family = Bytes.toBytes(fname);
    LOG.info("Creating table with " + nrs + " regions");
    HTable ht = TEST_UTIL.createTable(table, family);
    int numRegions = TEST_UTIL.createMultiRegions(conf, ht, family, nrs);
    assertEquals(nrs, numRegions);
      LOG.info("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    // disable-enable cycle to get rid of table's dead regions left behind
    // by createMultiRegions
    LOG.debug("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    NavigableSet<String> regions = getAllOnlineRegions(cluster);
    LOG.debug("Verifying only catalog and namespace regions are assigned\n");
    if (regions.size() != 2) {
      for (String oregion : regions)
        LOG.debug("Region still online: " + oregion);
    }
    assertEquals(2 + existingRegions, regions.size());
    LOG.debug("Enabling table\n");
    TEST_UTIL.getHBaseAdmin().enableTable(table);
    LOG.debug("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    LOG.debug("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = getAllOnlineRegions(cluster);
    assertEquals(numRegions + 2 + existingRegions, regions.size());
    return ht;
  }

  void populateDataInTable(int nrows, String fname) throws Exception {
    byte [] family = Bytes.toBytes(fname);

    List<RegionServerThread> rsts = cluster.getLiveRegionServerThreads();
    assertEquals(NUM_RS, rsts.size());

    for (RegionServerThread rst : rsts) {
      HRegionServer hrs = rst.getRegionServer();
      List<HRegionInfo> hris = ProtobufUtil.getOnlineRegions(hrs);
      for (HRegionInfo hri : hris) {
        if (hri.getTableName().getNamespaceAsString().equals(
            NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
          continue;
        }
        LOG.debug("adding data to rs = " + rst.getName() +
            " region = "+ hri.getRegionNameAsString());
        HRegion region = hrs.getOnlineRegion(hri.getRegionName());
        assertTrue(region != null);
        putData(region, hri.getStartKey(), nrows, Bytes.toBytes("q"), family);
      }
    }
  }

  public void makeHLog(HLog log, List<HRegionInfo> regions, String tname, String fname,
      int num_edits, int edit_size) throws IOException {
    makeHLog(log, regions, tname, fname, num_edits, edit_size, true);
  }

  public void makeHLog(HLog log, List<HRegionInfo> regions, String tname, String fname,
      int num_edits, int edit_size, boolean closeLog) throws IOException {
    TableName fullTName = TableName.valueOf(tname);
    // remove root and meta region
    regions.remove(HRegionInfo.FIRST_META_REGIONINFO);
    byte[] table = Bytes.toBytes(tname);
    HTableDescriptor htd = new HTableDescriptor(tname);
    byte[] value = new byte[edit_size];

    List<HRegionInfo> hris = new ArrayList<HRegionInfo>();
    for (HRegionInfo region : regions) {
      if (!region.getTableName().getNameAsString().equalsIgnoreCase(tname)) {
        continue;
      }
      hris.add(region);
    }
    for (int i = 0; i < edit_size; i++) {
      value[i] = (byte) ('a' + (i % 26));
    }
    int n = hris.size();
    int[] counts = new int[n];
    if (n > 0) {
      for (int i = 0; i < num_edits; i += 1) {
        WALEdit e = new WALEdit();
        HRegionInfo curRegionInfo = hris.get(i % n);
        byte[] startRow = curRegionInfo.getStartKey();
        if (startRow == null || startRow.length == 0) {
          startRow = new byte[] { 0, 0, 0, 0, 1 };
        }
        byte[] row = Bytes.incrementBytes(startRow, counts[i % n]);
        row = Arrays.copyOfRange(row, 3, 8); // use last 5 bytes because
                                             // HBaseTestingUtility.createMultiRegions use 5 bytes
                                             // key
        byte[] family = Bytes.toBytes(fname);
        byte[] qualifier = Bytes.toBytes("c" + Integer.toString(i));
        e.add(new KeyValue(row, family, qualifier, System.currentTimeMillis(), value));
        log.append(curRegionInfo, fullTName, e, System.currentTimeMillis(), htd);
        counts[i % n] += 1;
      }
    }
    log.sync();
    if(closeLog) {
      log.close();
    }
    for (int i = 0; i < n; i++) {
      LOG.info("region " + hris.get(i).getRegionNameAsString() + " has " + counts[i] + " edits");
    }
    return;
  }

  private int countHLog(Path log, FileSystem fs, Configuration conf)
  throws IOException {
    int count = 0;
    HLog.Reader in = HLogFactory.createReader(fs, log, conf);
    while (in.next() != null) {
      count++;
    }
    return count;
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master)
  throws KeeperException, InterruptedException {
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
  }

  private void putData(HRegion region, byte[] startRow, int numRows, byte [] qf,
      byte [] ...families)
  throws IOException {
    for(int i = 0; i < numRows; i++) {
      Put put = new Put(Bytes.add(startRow, Bytes.toBytes(i)));
      for(byte [] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  /**
   * Load table with puts and deletes with expected values so that we can verify later
   */
  private void prepareData(final HTable t, final byte[] f, final byte[] column) throws IOException {
    t.setAutoFlush(false);
    byte[] k = new byte[3];

    // add puts
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, column, k);
          t.put(put);
        }
      }
    }
    t.flushCommits();
    // add deletes
    for (byte b3 = 'a'; b3 <= 'z'; b3++) {
      k[0] = 'a';
      k[1] = 'a';
      k[2] = b3;
      Delete del = new Delete(k);
      t.delete(del);
    }
    t.flushCommits();
  }

  private NavigableSet<String> getAllOnlineRegions(MiniHBaseCluster cluster)
      throws IOException {
    NavigableSet<String> online = new TreeSet<String>();
    for (RegionServerThread rst : cluster.getLiveRegionServerThreads()) {
      for (HRegionInfo region : ProtobufUtil.getOnlineRegions(rst.getRegionServer())) {
        online.add(region.getRegionNameAsString());
      }
    }
    return online;
  }

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        Thread.yield();
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return;
      }
    }
    assertTrue(false);
  }

  private void abortMaster(MiniHBaseCluster cluster) throws InterruptedException {
    for (MasterThread mt : cluster.getLiveMasterThreads()) {
      if (mt.getMaster().isActiveMaster()) {
        mt.getMaster().abort("Aborting for tests", new Exception("Trace info"));
        mt.join();
        break;
      }
    }
    LOG.debug("Master is aborted");
  }

  private void startMasterAndWaitUntilLogSplit(MiniHBaseCluster cluster)
      throws IOException, InterruptedException {
    cluster.startMaster();
    HMaster master = cluster.getMaster();
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    ServerManager serverManager = master.getServerManager();
    while (serverManager.areDeadServersInProgress()) {
      Thread.sleep(100);
    }
  }
}
