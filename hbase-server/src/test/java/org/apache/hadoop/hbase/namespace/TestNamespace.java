package org.apache.hadoop.hbase.namespace;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.FullyQualifiedTableName;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
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
  private String prefix = "TestNamespace";


  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE);
    admin = TEST_UTIL.getHBaseAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();
    zkNamespaceManager =
        new ZKNamespaceManager(master.getZooKeeperWatcher());
    zkNamespaceManager.start();
    LOG.info("Done initializing cluster");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeMethod() throws IOException {
    for(HTableDescriptor desc: admin.listTables(prefix+".*")) {
      admin.disableTable(desc.getFullyQualifiedTableName());
      admin.deleteTable(desc.getFullyQualifiedTableName());
    }
    for(NamespaceDescriptor ns: admin.listNamespaceDescriptors()) {
      if (ns.getName().startsWith(prefix)) {
        admin.deleteNamespace(ns.getName());
      }
    }
  }

  @Test
  public void verifyReservedNS() throws IOException {
    //verify existence of reserved namespaces
    NamespaceDescriptor ns =
        admin.getNamespaceDescriptor(NamespaceDescriptor.DEFAULT_NAMESPACE.getName());
    assertNotNull(ns);
    assertEquals(ns.getName(), NamespaceDescriptor.DEFAULT_NAMESPACE.getName());
    assertNotNull(zkNamespaceManager.get(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR));

    ns = admin.getNamespaceDescriptor(NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertNotNull(ns);
    assertEquals(ns.getName(), NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertNotNull(zkNamespaceManager.get(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR));

    assertEquals(2, admin.listNamespaceDescriptors().size());

    //verify existence of system tables
    Set<FullyQualifiedTableName> systemTables = Sets.newHashSet(
        HConstants.META_TABLE_NAME,
        HConstants.NAMESPACE_TABLE_NAME);
    List<HTableDescriptor> descs =
        admin.getTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    assertEquals(systemTables.size(), descs.size());
    for(HTableDescriptor desc : descs) {
      assertTrue(systemTables.contains(desc.getFullyQualifiedTableName()));
    }
    //verify system tables aren't listed
    assertEquals(0, admin.listTables().length);
    
    //Try creating default and system namespaces. 
    boolean exceptionCaught = false;
    try {
    admin.createNamespace(NamespaceDescriptor.DEFAULT_NAMESPACE);
    } catch (IOException exp){
      LOG.warn(exp);
      exceptionCaught = true;
    }finally {
      assertTrue(exceptionCaught);
    }
    exceptionCaught = false;
    try {
    admin.createNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE);
    } catch (IOException exp){
      LOG.warn(exp);
      exceptionCaught = true;
    }finally {
      assertTrue(exceptionCaught);
    }
  }
  
  @Test
  public void testDeleteReservedNS() throws Exception {
    boolean exceptionCaught = false;
    try {
      admin.deleteNamespace(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    } catch (IOException exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
    }
    try {
      admin.deleteNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    } catch (IOException exp) {
      LOG.warn(exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void createRemoveTest() throws Exception {
    String testName = "createRemoveTest";
    String nsName = prefix+"_"+testName;
    LOG.info(testName);

    //create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    assertEquals(3, admin.listNamespaceDescriptors().size());
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return zkNamespaceManager.list().size() == 3;
      }
    });
    assertNotNull(zkNamespaceManager.get(nsName));
    //remove namespace and verify
    admin.deleteNamespace(nsName);
    assertEquals(2, admin.listNamespaceDescriptors().size());
    assertEquals(2, zkNamespaceManager.list().size());
    assertNull(zkNamespaceManager.get(nsName));
  }

  @Test
  public void createDottedNS() throws Exception {
    String testName = "createDottedNS";
    String nsName = prefix+"_"+testName+".dot2.dot3";
    LOG.info(testName);

    //create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    assertEquals(3, admin.listNamespaceDescriptors().size());
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return zkNamespaceManager.list().size() == 3;
      }
    });
    assertNotNull(zkNamespaceManager.get(nsName));
    //remove namespace and verify
    admin.deleteNamespace(nsName);
    assertEquals(2, admin.listNamespaceDescriptors().size());
    assertEquals(2, zkNamespaceManager.list().size());
    assertNull(zkNamespaceManager.get(nsName));
  }

  @Test
  public void createDoubleTest() throws IOException, InterruptedException {
    String testName = "createDoubleTest";
    String nsName = prefix+"_"+testName;
    LOG.info(testName);

    byte[] tableName = Bytes.toBytes("my_table");
    byte[] tableNameFoo = Bytes.toBytes(nsName+".my_table");
    //create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    TEST_UTIL.createTable(tableName, Bytes.toBytes(nsName));
    TEST_UTIL.createTable(tableNameFoo,Bytes.toBytes(nsName));
    assertEquals(2, admin.listTables().length);
    assertNotNull(admin
        .getTableDescriptor(tableName));
    assertNotNull(admin
        .getTableDescriptor(tableNameFoo));
    //remove namespace and verify
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    assertEquals(1, admin.listTables().length);
  }

  @Test
  public void createTableTest() throws IOException, InterruptedException {
    String testName = "createTableTest";
    String nsName = prefix+"_"+testName;
    LOG.info(testName);

    HTableDescriptor desc = new HTableDescriptor(nsName+".my_table");
    HColumnDescriptor colDesc = new HColumnDescriptor("my_cf");
    desc.addFamily(colDesc);
    try {
      admin.createTable(desc);
      fail("Expected no namespace constraint exception");
    } catch (ConstraintException ex) {
    }
    //create table and in new namespace
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    admin.createTable(desc);
    TEST_UTIL.waitTableAvailable(desc.getFullyQualifiedTableName().getName(), 10000);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    assertTrue(fs.exists(new Path(master.getMasterFileSystem().getRootDir(),
        new Path(HConstants.BASE_NAMESPACE_DIR, new Path(nsName, desc.getNameAsString())))));
    assertEquals(1, admin.listTables().length);

    //verify non-empty namespace can't be removed
    try {
      admin.deleteNamespace(nsName);
      fail("Expected non-empty namespace constraint exception");
    } catch (Exception ex) {
      LOG.info("Caught expected exception: "+ex);
    }

    //sanity check try to write and read from table
    HTable table = new HTable(TEST_UTIL.getConfiguration(), desc.getFullyQualifiedTableName());
    Put p = new Put(Bytes.toBytes("row1"));
    p.add(Bytes.toBytes("my_cf"),Bytes.toBytes("my_col"),Bytes.toBytes("value1"));
    table.put(p);
    //flush and read from disk to make sure directory changes are working
    admin.flush(desc.getFullyQualifiedTableName().getName());
    Get g = new Get(Bytes.toBytes("row1"));
    assertTrue(table.exists(g));

    //normal case of removing namespace
    TEST_UTIL.deleteTable(desc.getFullyQualifiedTableName());
    admin.deleteNamespace(nsName);
  }

  @Test
  public void createTableInDefaultNamespace() throws Exception {
    HTableDescriptor desc = new HTableDescriptor("default_table");
    HColumnDescriptor colDesc = new HColumnDescriptor("cf1");
    desc.addFamily(colDesc);
    admin.createTable(desc);
    assertTrue(admin.listTables().length == 1);  
    admin.disableTable(desc.getFullyQualifiedTableName());
    admin.deleteTable(desc.getFullyQualifiedTableName());
  }

  @Test
  public void createTableInSystemNamespace() throws Exception {
    String tableName = "hbase.createTableInSystemNamespace";
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDesc = new HColumnDescriptor("cf1");
    desc.addFamily(colDesc);
    admin.createTable(desc);
    assertEquals(0, admin.listTables().length);
    assertTrue(admin.tableExists(Bytes.toBytes(tableName)));
    admin.disableTable(desc.getFullyQualifiedTableName());
    admin.deleteTable(desc.getFullyQualifiedTableName());
  }

}
