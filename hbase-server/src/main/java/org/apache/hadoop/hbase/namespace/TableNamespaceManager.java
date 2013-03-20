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

package org.apache.hadoop.hbase.namespace;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.ConstraintException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.NamespaceProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

public class TableNamespaceManager {
  private static final Log LOG = LogFactory.getLog(TableNamespaceManager.class);

  public static String FAMILY_INFO = "info";
  public static byte[] FAMILY_INFO_BYTES = Bytes.toBytes("info");
  public static byte[] COL_DESCRIPTOR = Bytes.toBytes("descriptor");
  private static NamespaceDescriptor defaultNS =
      NamespaceDescriptor.create(NamespaceDescriptor.DEFAULT_NAMESPACE)
          .build();
  private static NamespaceDescriptor systemNS =
      NamespaceDescriptor.create(NamespaceDescriptor.SYSTEM_NAMESPACE)
          .build();

  private Configuration conf;
  private MasterServices masterServices;
  private HTable table;
  private ZKNamespaceManager zkNamespaceManager;

  public TableNamespaceManager(MasterServices masterServices) throws IOException {
    this.masterServices = masterServices;
    this.conf = masterServices.getConfiguration();
  }

  public void start() throws IOException {
    TableName tableName = TableName.valueOf(HConstants.NAMESPACE_TABLE_NAME);
    boolean newTable = false;
    try {
      if(!MetaReader.tableExists(masterServices.getCatalogTracker(), tableName.getNameAsString())) {
        LOG.info("Namespace table not found. Creating...");
        newTable = true;
        createNamespaceTable(masterServices);
      }
    } catch (InterruptedException e) {
      throw new IOException("Wait for namespace table assignment interrupted", e);
    }
    table = new HTable(conf, tableName.getName());
    zkNamespaceManager = new ZKNamespaceManager(masterServices.getZooKeeper());
    zkNamespaceManager.start();
    if (newTable) {
      create(defaultNS);
      create(systemNS);
    }

    for(Result result: table.getScanner(FAMILY_INFO_BYTES)) {
      NamespaceDescriptor ns =
          ProtobufUtil.toNamespaceDescriptor(
              NamespaceProtos.NamespaceDescriptor.parseFrom(
                  result.getColumnLatest(FAMILY_INFO_BYTES, COL_DESCRIPTOR).getValue()));
      zkNamespaceManager.update(ns);
    }
  }


  public NamespaceDescriptor get(String name) throws IOException {
    Result res = table.get(new Get(Bytes.toBytes(name)));
    if(res.isEmpty())
      return null;
    return
        ProtobufUtil.toNamespaceDescriptor(
            NamespaceProtos.NamespaceDescriptor.parseFrom(
                res.getColumnLatest(FAMILY_INFO_BYTES, COL_DESCRIPTOR).getValue()));
  }

  public void create(NamespaceDescriptor ns) throws IOException {
    if(get(ns.getName()) != null) {
      throw new ConstraintException("Namespace "+ns.getName()+" already exists");
    }
    FileSystem fs = masterServices.getMasterFileSystem().getFileSystem();
    fs.mkdirs(NamespaceDescriptor.getNamespaceDir(
        masterServices.getMasterFileSystem().getRootDir(), ns.getName()));
    upsert(ns);
  }

  public void update(NamespaceDescriptor ns) throws IOException {
    if(get(ns.getName()) == null) {
      throw new ConstraintException("Namespace "+ns.getName()+" does not exist");
    }
    upsert(ns);
  }

  private void upsert(NamespaceDescriptor ns) throws IOException {
    Put p = new Put(Bytes.toBytes(ns.getName()));
    p.add(FAMILY_INFO_BYTES, COL_DESCRIPTOR,
        ProtobufUtil.toProtoBuf(ns).toByteArray());
    table.put(p);
    zkNamespaceManager.update(ns);
  }

  public void remove(String name) throws IOException {
    if(name.equals(HConstants.DEFAULT_NAMESPACE_NAME_STR) ||
       name.equals(HConstants.SYSTEM_NAMESPACE_NAME_STR)) {
      throw new ConstraintException("Reserved namespace "+name+" cannot be removed.");
    }
    Delete d = new Delete(Bytes.toBytes(name));
    table.delete(d);
    zkNamespaceManager.remove(name);
    FileSystem fs = masterServices.getMasterFileSystem().getFileSystem();
    for(FileStatus status :
            fs.listStatus(NamespaceDescriptor.getNamespaceDir(
                masterServices.getMasterFileSystem().getRootDir(), name))) {
      if(!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
        throw new IOException("Namespace directory contains table dir: "+status.getPath());
      }
    }
    fs.delete(NamespaceDescriptor.getNamespaceDir(
        masterServices.getMasterFileSystem().getRootDir(), name), true);
  }

  public NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    for(Result r: table.getScanner(FAMILY_INFO_BYTES)) {
        ret.add(ProtobufUtil.toNamespaceDescriptor(
            NamespaceProtos.NamespaceDescriptor.parseFrom(
              r.getColumnLatest(FAMILY_INFO_BYTES, COL_DESCRIPTOR).getValue())));
    }
    return ret;
  }

  private void createNamespaceTable(MasterServices masterServices) throws IOException, InterruptedException {
    HRegionInfo newRegions[] = new HRegionInfo[]{
        new HRegionInfo(HTableDescriptor.NAMESPACE_TABLEDESC.getName(), null, null)};

    //we need to create the table this way to bypass
    //checkInitialized
    masterServices.getExecutorService()
        .submit(new CreateTableHandler(masterServices,
            masterServices.getMasterFileSystem(),
            HTableDescriptor.NAMESPACE_TABLEDESC,
            masterServices.getConfiguration(),
            newRegions,
            masterServices).prepare());
    //wait for region to be online
    int tries = 600;
    while(masterServices.getAssignmentManager()
        .getRegionStates().getRegionServerOfRegion(newRegions[0]) == null &&
        tries > 0) {
      Thread.sleep(100);
      tries--;
    }
    if(tries <= 0) {
      throw new IOException("Failed to create namespace table.");
    }
  }
}
