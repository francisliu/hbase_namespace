/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.namespace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.RegionOfflineException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestNamespaceController {
  private static final Log LOG = LogFactory.getLog(TestNamespaceController.class);
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  protected static HBaseAdmin admin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, NamespaceController.class.getName());
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, NamespaceController.class.getName());
    UTIL.startMiniCluster();
    admin = UTIL.getHBaseAdmin();
    ZKNamespaceManager zkNamespaceManager = new ZKNamespaceManager(UTIL.getZooKeeperWatcher());
    zkNamespaceManager.start();
    LOG.info("Done initializing cluster");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTableQuota() throws Exception {
    String nsp1 = "np1";
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create(nsp1)
        .addConfiguration(NamespaceController.KEY_MAX_REGIONS, "5")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    int size = admin.listNamespaceDescriptors().size();
    assertEquals(3, size);
    HTableDescriptor tableDescOne = new HTableDescriptor(nsp1 + "." + "table1");
    HTableDescriptor tableDescTwo = new HTableDescriptor(nsp1 + "." + "table2");
    HTableDescriptor tableDescThree = new HTableDescriptor(nsp1 + "." + "table3");
    admin.createTable(tableDescOne);
    boolean constraintViolated = false;
    try {
      //This should fail as the region quota will be violated.
      admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 5);
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue(constraintViolated);
    }
    //This should pass.
    admin.createTable(tableDescTwo, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 4);
    constraintViolated = false;
    try {
      admin.createTable(tableDescThree);
    } catch (Exception exp) {
      assertTrue(exp instanceof IOException);
      constraintViolated = true;
    } finally {
      assertTrue(constraintViolated);
    }
    admin.disableTable(tableDescOne.getName());
    admin.deleteTable(tableDescOne.getName());
    admin.disableTable(tableDescTwo.getName());
    admin.deleteTable(tableDescTwo.getName());
    admin.deleteNamespace(nsp1);
  }

  @Test
  public void testRegionQuota() throws Exception {
    String nsp1 = "np2";
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create(nsp1)
        .addConfiguration(NamespaceController.KEY_MAX_REGIONS, "6")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    // System and np2
    assertEquals( admin.listNamespaceDescriptors().size(), 3);
    boolean constraintViolated = false;
    HTableDescriptor tableDescOne = new HTableDescriptor(nsp1 + "." + "table1");
    tableDescOne.addFamily(new HColumnDescriptor(Bytes.toBytes("info")));
    try {
      admin.createTable(tableDescOne, Bytes.toBytes("AAA"), Bytes.toBytes("ZZZ"), 7);
    } catch (Exception exp) {
      assertTrue(exp instanceof DoNotRetryIOException);
      LOG.info(exp);
      constraintViolated = true;
    } finally {
      assertTrue(constraintViolated);
    }
    assertFalse(admin.tableExists(tableDescOne.getName()));

    // This call will pass.
    admin.createTable(tableDescOne, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 5);
    HTable table = new HTable(conf, tableDescOne.getName());
    NavigableMap<HRegionInfo, ServerName> regionLocations = table.getRegionLocations();
    assertEquals(5,regionLocations.keySet().size());
    int rowCount = UTIL.loadTable(table, Bytes.toBytes("info"));
    assertEquals(rowCount, UTIL.countRows(table));
    admin.split(tableDescOne.getName());
    int count = waitForRegionsToSettle(regionLocations.size(), tableDescOne.getName());
    assertEquals(6, count);
    // This will not pass.
    admin.split(tableDescOne.getName());
    count = waitForRegionsToSettle(regionLocations.size(), tableDescOne.getName());
    assertEquals(rowCount, UTIL.countRows(table));
    table.close();
    admin.disableTable(tableDescOne.getName());
    admin.deleteTable(tableDescOne.getName());
    admin.deleteNamespace(nsp1);
  }

  private int waitForRegionsToSettle(int originalCount, byte[] tableName)
      throws InterruptedException, IOException {
    int numRetries = 4;
    while (numRetries >= 0) {
      Thread.sleep(2000);
      try {
        if (MetaScanner.allTableRegions(UTIL.getConfiguration(), tableName, false).size() > originalCount) break;
      } catch (RegionOfflineException exp) {
        LOG.warn(exp);
      }
      numRetries--;
    }
    return MetaScanner.allTableRegions(UTIL.getConfiguration(), tableName, false).size();
  }

  @Test
  public void testValidQuotas() throws Exception {
    boolean exceptionCaught = false;
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    NamespaceDescriptor nspDesc = NamespaceDescriptor.create("vq1")
        .addConfiguration(NamespaceController.KEY_MAX_REGIONS, "hihdufh")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "2").build();
    try {
      admin.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc = NamespaceDescriptor.create("vq2")
        .addConfiguration(NamespaceController.KEY_MAX_REGIONS, "-456")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "2").build();
    try {
      admin.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc = NamespaceDescriptor.create("vq3").addConfiguration(NamespaceController.KEY_MAX_REGIONS, "10")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "sciigd").build();
    try {
      admin.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    nspDesc = NamespaceDescriptor.create("vq4").addConfiguration(NamespaceController.KEY_MAX_REGIONS, "10")
        .addConfiguration(NamespaceController.KEY_MAX_TABLES, "-1500").build();
    try {
      admin.createNamespace(nspDesc);
    } catch (Exception exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
      assertFalse(fs.exists(FSUtils.getNamespaceDir(rootDir, nspDesc.getName())));
    }
    assertTrue(admin.listNamespaceDescriptors().size() == 2);
  }
}
