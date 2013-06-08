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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.AccessControlService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.BlockingRpcChannel;

@Category(MediumTests.class)
@SuppressWarnings("rawtypes")
public class TestNamespaceCommands extends SecureTestUtil {
  static HBaseTestingUtility UTIL= new HBaseTestingUtility();
  private static String TestNamespace = "ns1";
  static Configuration conf;
  static MasterCoprocessorEnvironment CP_ENV;
  static RegionCoprocessorEnvironment RCP_ENV;
  static RegionServerCoprocessorEnvironment RSCP_ENV;
  static AccessController ACCESS_CONTROLLER;
  private static HTable ACL_TABLE;
  
//user with all permissions
  static User SUPERUSER;
 // user with rw permissions
  static User USER_RW;
 // user with create table permissions alone
  static User USER_CREATE;
  // user with permission on namespace for testing all operations.
  static User USER_NSP;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    UTIL.startMiniCluster();
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_RW = User.createUserForTesting(conf, "rw_user", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "create_user", new String[0]);
    USER_NSP = User.createUserForTesting(conf, "namespace_admin", new String[0]);
    UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(TestNamespace).build());

    // Wait for the ACL table to become available
    UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 8000);

    ACL_TABLE = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    MasterCoprocessorHost cpHost = UTIL.getMiniHBaseCluster().getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    try {
      BlockingRpcChannel service = ACL_TABLE.coprocessorService(TEST_TABLE);
      AccessControlService.BlockingInterface protocol =
        AccessControlService.newBlockingStub(service);
      ProtobufUtil.grant(protocol, USER_NSP.getShortName(), AccessControlLists
      .getNamespaceEntry(TestNamespace), new byte[0], new byte[0], Permission.Action.READ,
      Permission.Action.WRITE, Permission.Action.EXEC, Permission.Action.CREATE,
      Permission.Action.ADMIN); 
    } finally {
      ACL_TABLE.close();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.getHBaseAdmin().deleteNamespace(TestNamespace);
    ACL_TABLE.close();
  }

  @Test
  public void testGrantRevoke() throws Exception {
    User user_test_nsp = User.createUserForTesting(conf, "user_test_nsp", new String[0]);
    try {
    AccessControllerProtocol protocol = ACL_TABLE.coprocessorProxy(AccessControllerProtocol.class,
      AccessControlLists.ACL_TABLE_NAME);
    grant(protocol, user_test_nsp, AccessControlLists.getNamespaceEntry(TestNamespace), null, null,
      Permission.Action.ADMIN);
    Result result = ACL_TABLE.get(new Get(Bytes.toBytes(user_test_nsp.getShortName())));
    assertTrue(result != null);
    List<UserPermission> perms = protocol.getUserPermissions(AccessControlLists
        .getNamespaceEntry(TestNamespace));
    assertEquals(2, perms.size());
    UserPermission toCheck = perms.get(0);
    assertEquals(user_test_nsp.getShortName(), Bytes.toString(toCheck.getUser()));
    assertEquals(Bytes.toString(AccessControlLists.getNamespaceEntry(TestNamespace)),
      Bytes.toString(toCheck.getTable()));
    assertEquals(null, toCheck.getFamily());
    assertEquals(null, toCheck.getQualifier());
    assertEquals(1, toCheck.getActions().length);
    assertEquals(Permission.Action.ADMIN, toCheck.getActions()[0]);
    
    // Now revoke and check.
    protocol.revoke(new UserPermission(Bytes.toBytes(user_test_nsp.getShortName()),
        AccessControlLists.getNamespaceEntry(TestNamespace), new byte[0], Permission.Action.ADMIN));
    perms = protocol.getUserPermissions(AccessControlLists.getNamespaceEntry(TestNamespace));
    assertEquals(1, perms.size());
    } finally {
      ACL_TABLE.close();
    }
  }
  
  @Test
  public void testTableCreate() throws Exception { 
    PrivilegedExceptionAction createTable = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TestNamespace + ".testnewtable");
        htd.addFamily(new HColumnDescriptor("TestFamily"));
        ACCESS_CONTROLLER.preCreateTable(ObserverContext.createAndPrepare(CP_ENV, null), htd, null);
        return null;
      }
    };
    // verify that superuser and namespace admin can create tables
    verifyAllowed(createTable, SUPERUSER, USER_NSP);
    // all others should be denied
    verifyDenied(createTable, USER_CREATE, USER_RW);
  }
  
  @Test
  public void testModifyNamespace() throws Exception {
    PrivilegedExceptionAction modifyNamespace = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        ACCESS_CONTROLLER.preModifyNamespace(ObserverContext.createAndPrepare(CP_ENV, null),
          NamespaceDescriptor.create(TestNamespace).addConfiguration("abc", "156").build());
        return null;
      }
    };
    // verify that superuser and namespace admin can create tables
    verifyAllowed(modifyNamespace, SUPERUSER);
    // all others should be denied
    verifyDenied(modifyNamespace, USER_NSP, USER_CREATE, USER_RW);
  }
}
