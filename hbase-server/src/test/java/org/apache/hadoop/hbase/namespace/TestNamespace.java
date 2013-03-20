package org.apache.hadoop.hbase.namespace;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class TestNamespace {
  protected static final Log LOG = LogFactory.getLog(TestNamespace.class);
  private static HMaster master;
  protected final static int NUM_SLAVES_BASE = 4;
  private static HBaseTestingUtility TEST_UTIL;
  protected static HBaseAdmin admin;
  protected static HBaseCluster cluster;
  private static ZKNamespaceManager zkNamespaceManager;


  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE);
    admin = TEST_UTIL.getHBaseAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();
    zkNamespaceManager =
        new ZKNamespaceManager(admin.getConnection().getZooKeeperWatcher());
    zkNamespaceManager.start();
    LOG.info("Done initializing cluster");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void verifyReservedNS() throws IOException {
    //verify existence of reserved namespaces
    NamespaceDescriptor ns =
        admin.getNamespaceDescriptor(NamespaceDescriptor.DEFAULT_NAMESPACE.getName());
    assertNotNull(ns);
    assertEquals(ns.getName(), NamespaceDescriptor.DEFAULT_NAMESPACE.getName());
    assertNotNull(zkNamespaceManager.get(HConstants.DEFAULT_NAMESPACE_NAME_STR));

    ns = admin.getNamespaceDescriptor(NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertNotNull(ns);
    assertEquals(ns.getName(), NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertNotNull(zkNamespaceManager.get(HConstants.SYSTEM_NAMESPACE_NAME_STR));

    assertEquals(2, admin.listNamespaceDescriptors().size());

    //verify existence of system tables
    Set<String> systemTables = Sets.newHashSet(
        Bytes.toString(HConstants.META_TABLE_NAME),
        Bytes.toString(HConstants.NAMESPACE_TABLE_NAME));
    List<HTableDescriptor> descs =
        admin.getTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertEquals(systemTables.size(), descs.size());
    for(HTableDescriptor desc : descs) {
      assertTrue(systemTables.contains(desc.getNameAsString()));
    }
    //verify system tables aren't listed
    assertEquals(0, admin.listTables().length);
  }

  @Test
  public void createRemoveTest() throws IOException, InterruptedException {
    //create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create("foo").build());
    assertEquals(3, admin.listNamespaceDescriptors().size());
    assertEquals(3, zkNamespaceManager.list().size());
    assertNotNull(zkNamespaceManager.get("foo"));
    //remove namespace and verify
    admin.deleteNamespace("foo");
    assertEquals(2, admin.listNamespaceDescriptors().size());
    assertEquals(2, zkNamespaceManager.list().size());
    assertNull(zkNamespaceManager.get("foo"));
  }

  @Test
  public void createDoubleTest() throws IOException, InterruptedException {
    byte[] tableName = Bytes.toBytes("my_table");
    byte[] tableNameFoo = Bytes.toBytes("foo.my_table");
    //create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create("foo").build());
    TEST_UTIL.createTable(tableName,Bytes.toBytes("f"));
    TEST_UTIL.createTable(tableNameFoo,Bytes.toBytes("f"));
    assertEquals(2, admin.listTables().length);
    assertNotNull(admin
        .getTableDescriptor(tableName));
    assertNotNull(admin
        .getTableDescriptor(tableNameFoo));
    //remove namespace and verify
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    assertEquals(1, admin.listTables().length);
    admin.disableTable(tableNameFoo);
    admin.deleteTable(tableNameFoo);
    admin.deleteNamespace("foo");
  }

  @Test
  public void createTableTest() throws IOException, InterruptedException {
    HTableDescriptor desc = new HTableDescriptor("bar.my_table");
    HColumnDescriptor colDesc = new HColumnDescriptor("my_cf");
    desc.addFamily(colDesc);
    try {
      admin.createTable(desc);
      fail("Expected no namespace constraint exception");
    } catch (ConstraintException ex) {
    }
    //create table and in new namespace
    admin.createNamespace(NamespaceDescriptor.create("bar").build());
    admin.createTable(desc);
    TEST_UTIL.waitTableAvailable(desc.getName(), 10000);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    assertTrue(fs.exists(new Path(master.getMasterFileSystem().getRootDir(),
        new Path(HConstants.BASE_NAMESPACE_DIR, new Path("bar", "my_table")))));
    assertEquals(1, admin.listTables().length);

    //verify non-empty namespace can't be removed
    try {
      admin.deleteNamespace("bar");
      fail("Expected non-empty namespace constraint exception");
    } catch (Exception ex) {
    }

    //sanity check try to write and read from table
    HTable table = new HTable(TEST_UTIL.getConfiguration(), desc.getName());
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("my_cf"),Bytes.toBytes("my_col"),Bytes.toBytes("value1"));
    table.put(p);
    //flush and read from disk to make sure directory changes are working
    admin.flush(desc.getName());
    Get g = new Get(Bytes.toBytes("row1"));
    assertTrue(table.exists(g));

    //normal case of removing namespace
    TEST_UTIL.deleteTable(desc.getName());
    admin.deleteNamespace("bar");
  }


}
