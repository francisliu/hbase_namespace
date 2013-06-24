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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.ConstraintException;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

public class NamespaceController extends BaseRegionObserver implements MasterObserver {
  public static String KEY_MAX_REGIONS = "hbase.namespace.quota.maxregions";
  public static String KEY_MAX_TABLES = "hbase.namespace.quota.maxtables";
  private ZKNamespaceManager zkManager;
  
  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    ZooKeeperWatcher zk = null;
    if (e instanceof MasterCoprocessorEnvironment) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) e;
      zk = mEnv.getMasterServices().getZooKeeper();
    } else if (e instanceof RegionServerCoprocessorEnvironment) {
      RegionServerCoprocessorEnvironment rsEnv = (RegionServerCoprocessorEnvironment) e;
      zk = rsEnv.getRegionServerServices().getZooKeeper();
    } else if (e instanceof RegionCoprocessorEnvironment) {
      // if running at region
      RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e;
      zk = regionEnv.getRegionServerServices().getZooKeeper();
    }
    zkManager = new ZKNamespaceManager(zk);
    zkManager.start();
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    checkRegionQuota(e);
  }

  @Override
  public void preSplit(ObserverContext<RegionCoprocessorEnvironment> c, byte[] splitRow)
      throws IOException {
    checkRegionQuota(c);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    checkTableQuota(ctx, desc, regions);    
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    checkTableQuota(ctx, desc, regions);  
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException { 
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException { 
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException { 
  }

  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException { 
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HTableDescriptor htd) throws IOException { 
  }

  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HTableDescriptor htd) throws IOException { 
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor column) throws IOException { 
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor column) throws IOException { 
  }

  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException { 
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException { 
  }

  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      byte[] c) throws IOException {   
  }

  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName, byte[] c) throws IOException {  
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException { 
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
      byte[] tableName) throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
      throws IOException {
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean newValue) throws IOException { 
    return false;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {
  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException { 
  }

  @Override
  public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor) throws IOException {  
  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException { 
  }

  @Override
  public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException { 
    validateQuotaValues(ns);
  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
      throws IOException {
  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String namespace) throws IOException {
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {   
    validateQuotaValues(ns);
  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
      NamespaceDescriptor ns) throws IOException {
  }
  
  private NamespaceQuota getNamespaceQuota(Configuration conf, String namespace) throws IOException {
    List<HRegionInfo> regions = MetaScanner.listAllRegions(conf);
    Set<String> tables = new HashSet<String>();
    int regionCount = 0;
    for (HRegionInfo region : regions) {
      TableName name = TableName.valueOf(region.getTableName());
      if (name.getNamespaceAsString().equalsIgnoreCase(namespace)) {
        tables.add(name.getNameAsString());
        regionCount++;
      }
    }
    return new NamespaceQuota(namespace, tables, regionCount);
  }

  private void validateQuotaValues(NamespaceDescriptor desc) throws IOException {
    if (getMaxRegions(desc) <= 0) {
      throw new ConstraintException("The max region quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
    if (getMaxTables(desc) <= 0) {
      throw new ConstraintException("The max tables quota for " + desc.getName()
          + " is less than or equal to zero.");
    }
  }
  
  private static long getMaxRegions(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_REGIONS);
    long maxRegions = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxRegions = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new ConstraintException("NumberFormatException while getting max regions.", exp);
      }
    } else {
      // The property if not set, so assume its the max long value.
      maxRegions = Long.MAX_VALUE;
    }
    return maxRegions;
  }

  private static long getMaxTables(NamespaceDescriptor ns) throws IOException {
    String value = ns.getConfigurationValue(KEY_MAX_TABLES);
    long maxTables = 0;
    if (StringUtils.isNotEmpty(value)) {
      try {
        maxTables = Long.parseLong(value);
      } catch (NumberFormatException exp) {
        throw new ConstraintException("NumberFormatException while getting max tables.", exp);
      }
    } else {
      // The property if not set, so assume its the max long value.
      maxTables = Long.MAX_VALUE;
    }
    return maxTables;
  }
  
  private void checkTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    String namespace = desc.getTableName().getNamespaceAsString();
    NamespaceDescriptor nspdesc = zkManager.get(namespace);
    if (nspdesc != null) {
      NamespaceQuota currentStatus;
      try {
        currentStatus = getNamespaceQuota(ctx.getEnvironment().getConfiguration(),
          nspdesc.getName());
      } catch (IOException exp) {
        throw new DoNotRetryIOException("Unable to obtain current namespace quota.", exp);
      }
      if ((currentStatus.getTables().size()) >= getMaxTables(nspdesc)) {
        throw new DoNotRetryIOException("The table " + desc.getTableName().getNameAsString()
            + "cannot be created as it would exceed maximum number of tables allowed "
            + " in the namespace.");
      }

      if ((currentStatus.getRegionCount() + regions.length) > getMaxRegions(nspdesc)) {
        throw new DoNotRetryIOException("The table " + desc.getTableName().getNameAsString()
            + " is not allowed to have " + regions.length
            + " number of regions. The total number of regions permitted are only "
            + getMaxRegions(nspdesc) + ", while current region quota is "
            + currentStatus.getRegionCount()
            + ". This may be transient, please retry later if there are any"
            + " ongoing split operations in the namespace.");
      }
    }
  }
  
  private void checkRegionQuota(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
    RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment) e.getEnvironment();
    TableName tName = regionEnv.getRegion().getTableDesc().getTableName();
    NamespaceDescriptor nspdesc = zkManager.get(tName.getNamespaceAsString());
    if (nspdesc != null) {
      NamespaceQuota currentStatus;
      try {
        currentStatus = getNamespaceQuota(regionEnv.getConfiguration(),
          tName.getNamespaceAsString());
      } catch (IOException exp) {
        throw new DoNotRetryIOException("Unable to obtain current namespace quota.", exp);
      }
      if (currentStatus.getRegionCount() >= getMaxRegions(nspdesc)) {
        throw new DoNotRetryIOException("The region "
            + Bytes.toString(regionEnv.getRegion().getRegionName())
            + " cannot be created. The region count  will exceed quota on the namespace. "
            + "This may be transient, please retry later if there are any ongoing split"
            + " operations in the namespace.");
      }
    }
  }
}
