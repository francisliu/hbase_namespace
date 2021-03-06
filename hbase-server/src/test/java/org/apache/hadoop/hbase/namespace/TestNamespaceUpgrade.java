/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.namespace;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test upgrade from no namespace in 0.94 to namespace directory structure.
 * Mainly tests that tables are migrated and consistent. Also verifies
 * that snapshots have been migrated correctly.
 *
 * Uses a tarball which is an image of an 0.94 hbase.rootdir.
 *
 * Contains tables with currentKeys as the stored keys:
 * foo, ns1.foo, ns2.foo
 *
 * Contains snapshots with snapshot{num}Keys as the contents:
 * snapshot1Keys, snapshot2Keys
 *
 */
@Category(MediumTests.class)
public class TestNamespaceUpgrade {
  static final Log LOG = LogFactory.getLog(TestNamespaceUpgrade.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String snapshot1Keys[] =
      {"1","10","2","3","4","5","6","7","8","9"};
  private final static String snapshot2Keys[] =
      {"1","2","3","4","5","6","7","8","9"};
  private final static String currentKeys[] =
      {"1","2","3","4","5","6","7","8","9","A"};
  private final static String tables[] = {"foo", "ns1.foo","ns.two.foo"};

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start up our mini cluster on top of an 0.94 root.dir that has data from
    // a 0.94 hbase run and see if we can migrate to 0.96
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    Path testdir = TEST_UTIL.getDataTestDir("TestNamespaceUpgrade");
    // Untar our test dir.
    File untar = untar(new File(testdir.toString()));
    // Now copy the untar up into hdfs so when we start hbase, we'll run from it.
    Configuration conf = TEST_UTIL.getConfiguration();
    FsShell shell = new FsShell(conf);
    FileSystem fs = FileSystem.get(conf);
    // find where hbase will root itself, so we can copy filesystem there
    Path hbaseRootDir = TEST_UTIL.getDefaultRootDirPath();
    if (!fs.isDirectory(hbaseRootDir.getParent())) {
      // mkdir at first
      fs.mkdirs(hbaseRootDir.getParent());
    }
    doFsCommand(shell,
      new String [] {"-put", untar.toURI().toString(), hbaseRootDir.toString()});
    // See whats in minihdfs.
    doFsCommand(shell, new String [] {"-lsr", "/"});
    TEST_UTIL.startMiniHBaseCluster(1, 1);


    for(String table: tables) {
      int count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), table).getScanner(new Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      Assert.assertEquals(currentKeys.length, count);
    }
    assertNotNull(TEST_UTIL.getHBaseAdmin().getNamespaceDescriptor("ns1"));
    assertNotNull(TEST_UTIL.getHBaseAdmin().getNamespaceDescriptor("ns.two"));
  }

  private static File untar(final File testdir) throws IOException {
    // Find the src data under src/test/data
    final String datafile = "TestNamespaceUpgrade";
    File srcTarFile = new File(
      System.getProperty("project.build.testSourceDirectory", "src/test") +
      File.separator + "data" + File.separator + datafile + ".tgz");
    File homedir = new File(testdir.toString());
    File tgtUntarDir = new File(homedir, "hbase");
    if (tgtUntarDir.exists()) {
      if (!FileUtil.fullyDelete(tgtUntarDir)) {
        throw new IOException("Failed delete of " + tgtUntarDir.toString());
      }
    }
    if (!srcTarFile.exists()) {
      throw new IOException(srcTarFile+" does not exist");
    }
    LOG.info("Untarring " + srcTarFile + " into " + homedir.toString());
    FileUtil.unTar(srcTarFile, homedir);
    Assert.assertTrue(tgtUntarDir.exists());
    return tgtUntarDir;
  }

  private static void doFsCommand(final FsShell shell, final String [] args)
  throws Exception {
    // Run the 'put' command.
    int errcode = shell.run(args);
    if (errcode != 0) throw new IOException("Failed put; errcode=" + errcode);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSnapshots() throws IOException, InterruptedException {
    String snapshots[][] = {snapshot1Keys, snapshot2Keys};
    for(int i=1; i<=snapshots.length; i++) {
      for(String table: tables) {
        TEST_UTIL.getHBaseAdmin().cloneSnapshot(table+"_snapshot"+i, table+"_clone"+i);
        FSUtils.logFileSystemState(FileSystem.get(TEST_UTIL.getConfiguration()),
            FSUtils.getRootDir(TEST_UTIL.getConfiguration()),
            LOG);
        int count = 0;
        for(Result res: new HTable(TEST_UTIL.getConfiguration(), table+"_clone"+i).getScanner(new
            Scan())) {
          assertEquals(snapshots[i-1][count++], Bytes.toString(res.getRow()));
        }
        Assert.assertEquals(table+"_snapshot"+i, snapshots[i-1].length, count);
      }
    }
  }

  @Test
  public void testRenameUsingSnapshots() throws IOException, InterruptedException {
    String newNS = "newNS";
    TEST_UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(newNS).build());
    for(String table: tables) {
      TEST_UTIL.getHBaseAdmin().snapshot(table+"_snapshot3", table);
      String newTableName = newNS+"."+table.replaceAll("[.]","_")+"_clone3";
      TEST_UTIL.getHBaseAdmin().cloneSnapshot(table+"_snapshot3", newTableName);
      int count = 0;
      for(Result res: new HTable(TEST_UTIL.getConfiguration(), newTableName).getScanner(new
          Scan())) {
        assertEquals(currentKeys[count++], Bytes.toString(res.getRow()));
      }
      Assert.assertEquals(newTableName, currentKeys.length, count);
    }
  }
}

