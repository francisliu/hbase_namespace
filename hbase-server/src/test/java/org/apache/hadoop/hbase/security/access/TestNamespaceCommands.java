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

import org.apache.commons.lang.ArrayUtils;
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

import com.google.common.collect.ListMultimap;
import com.google.protobuf.BlockingRpcChannel;

@Category(MediumTests.class)
@SuppressWarnings("rawtypes")
public class TestNamespaceCommands extends SecureTestUtil {
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static String TestNamespace = "ns1";
  private static Configuration conf;
  private static MasterCoprocessorEnvironment CP_ENV;
  private static AccessController ACCESS_CONTROLLER;
  
//user with all permissions
  private static User SUPERUSER;
 // user with rw permissions
  private static User USER_RW;
 // user with create table permissions alone
  private static User USER_CREATE;
  // user with permission on namespace for testing all operations.
  private static User USER_NSP_ADMIN;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, AccessController.class.getName());
    UTIL.startMiniCluster();
    SUPERUSER = User.createUserForTesting(conf, "admin", new String[] { "supergroup" });
    USER_RW = User.createUserForTesting(conf, "rw_user", new String[0]);
    USER_CREATE = User.createUserForTesting(conf, "create_user", new String[0]);
    USER_NSP_ADMIN = User.createUserForTesting(conf, "namespace_admin", new String[0]);
    UTIL.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(TestNamespace).build());

    // Wait for the ACL table to become available
    UTIL.waitTableAvailable(AccessControlLists.ACL_TABLE_NAME, 8000);

    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    MasterCoprocessorHost cpHost = UTIL.getMiniHBaseCluster().getMaster().getCoprocessorHost();
    cpHost.load(AccessController.class, Coprocessor.PRIORITY_HIGHEST, conf);
    ACCESS_CONTROLLER = (AccessController) cpHost.findCoprocessor(AccessController.class.getName());
    try {
      BlockingRpcChannel service = acl.coprocessorService(AccessControlLists.ACL_TABLE_NAME);
      AccessControlService.BlockingInterface protocol =
        AccessControlService.newBlockingStub(service);
      ProtobufUtil.grant(protocol, USER_NSP_ADMIN.getShortName(), AccessControlLists
      .getNamespaceEntry(TestNamespace), new byte[0], new byte[0], Permission.Action.ADMIN); 
    } finally {
      acl.close();
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.getHBaseAdmin().deleteNamespace(TestNamespace);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAclTableEntries() throws Exception {
    String userTestNamespace = "userTestNsp";
    AccessControlService.BlockingInterface protocol = null;
    HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
    try {
      BlockingRpcChannel service = acl.coprocessorService(AccessControlLists.ACL_TABLE_NAME);
      protocol = AccessControlService.newBlockingStub(service);
      ProtobufUtil.grant(protocol, userTestNamespace,
        AccessControlLists.getNamespaceEntry(TestNamespace), ArrayUtils.EMPTY_BYTE_ARRAY,
        ArrayUtils.EMPTY_BYTE_ARRAY, Permission.Action.WRITE);
      Result result = acl.get(new Get(Bytes.toBytes(userTestNamespace)));
      assertTrue(result != null);
      ListMultimap<String, TablePermission> perms = AccessControlLists.getTablePermissions(conf,
        AccessControlLists.getNamespaceEntry(TestNamespace));
      assertEquals(2, perms.size());
      List<TablePermission> namespacePerms = perms.get(userTestNamespace);
      assertTrue(perms.containsKey(userTestNamespace));
      assertEquals(1, namespacePerms.size());
      assertEquals(Bytes.toString(AccessControlLists.getNamespaceEntry(TestNamespace)),
        Bytes.toString(namespacePerms.get(0).getTable()));
      assertEquals(null, namespacePerms.get(0).getFamily());
      assertEquals(null, namespacePerms.get(0).getQualifier());
      assertEquals(1, namespacePerms.get(0).getActions().length);
      assertEquals(Permission.Action.WRITE, namespacePerms.get(0).getActions()[0]);
      // Now revoke and check.
      ProtobufUtil.revoke(protocol, userTestNamespace,
        AccessControlLists.getNamespaceEntry(TestNamespace), ArrayUtils.EMPTY_BYTE_ARRAY,
        ArrayUtils.EMPTY_BYTE_ARRAY, Permission.Action.WRITE);
      perms = AccessControlLists.getTablePermissions(conf,
        AccessControlLists.getNamespaceEntry(TestNamespace));
      assertEquals(1, perms.size());
    } finally {
      acl.close();
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
    // verify that superuser and namespace admin can create tables in namespace.
    verifyAllowed(createTable, SUPERUSER, USER_NSP_ADMIN);
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
    // verify that superuser or hbase admin can modify namespaces.
    verifyAllowed(modifyNamespace, SUPERUSER);
    // all others should be denied
    verifyDenied(modifyNamespace, USER_NSP_ADMIN, USER_CREATE, USER_RW);
  }
  
  @Test
  public void testGrantRevoke() throws Exception{
    //Only HBase super user should be able to grant and revoke permissions to
    // namespaces.
    final String testUser = "testUser";
    PrivilegedExceptionAction grantAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(AccessControlLists.ACL_TABLE_NAME);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.grant(protocol, testUser, AccessControlLists.getNamespaceEntry(TestNamespace),
            ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, Action.WRITE);
        } finally {
          acl.close();
        }
        return null;
      }
    };

    PrivilegedExceptionAction revokeAction = new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        HTable acl = new HTable(conf, AccessControlLists.ACL_TABLE_NAME);
        try {
          BlockingRpcChannel service = acl.coprocessorService(AccessControlLists.ACL_TABLE_NAME);
          AccessControlService.BlockingInterface protocol =
            AccessControlService.newBlockingStub(service);
          ProtobufUtil.revoke(protocol, testUser, AccessControlLists.getNamespaceEntry(TestNamespace),
            ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, Action.WRITE);
        } finally {
          acl.close();
        }
        return null;
      }
    };
    
    verifyAllowed(grantAction, SUPERUSER);
    verifyDenied(grantAction, USER_CREATE, USER_RW);

    verifyAllowed(revokeAction, SUPERUSER);
    verifyDenied(revokeAction, USER_CREATE, USER_RW);
    
  }
}
