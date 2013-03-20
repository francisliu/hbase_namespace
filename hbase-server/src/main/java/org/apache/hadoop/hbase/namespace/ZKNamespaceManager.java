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

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.NamespaceProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;

class ZKNamespaceManager extends ZooKeeperListener {
  private static Log LOG = LogFactory.getLog(ZKNamespaceManager.class);
  private static String namespaceZNode = "namespace";
  private final String nsZNode;
  private NavigableMap<String,NamespaceDescriptor> cache;

  public ZKNamespaceManager(ZooKeeperWatcher zkw) throws IOException {
    super(zkw);
    nsZNode = ZKUtil.joinZNode(zkw.baseZNode, namespaceZNode);
    cache = new ConcurrentSkipListMap<String, NamespaceDescriptor>();
  }

  public void start() throws IOException {
    watcher.registerListener(this);
    try {
      if (ZKUtil.watchAndCheckExists(watcher, nsZNode)) {
        List<ZKUtil.NodeAndData> existing =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        if (existing != null) {
          refreshNodes(existing);
        }
      } else {
        ZKUtil.createWithParents(watcher, nsZNode);
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to initialize ZKNamespaceManager", e);
    }
  }

  public NamespaceDescriptor get(String name) {
    return cache.get(name);
  }

  public void update(NamespaceDescriptor ns) throws IOException {
    writeNamespace(ns);
    cache.put(ns.getName(), ns);
  }

  public void remove(String name) throws IOException {
    deleteNamespace(name);
    cache.remove(name);
  }

  public NavigableSet<NamespaceDescriptor> list() throws IOException {
    NavigableSet<NamespaceDescriptor> ret =
        Sets.newTreeSet(NamespaceDescriptor.NAMESPACE_DESCRIPTOR_COMPARATOR);
    for(NamespaceDescriptor ns: cache.values()) {
      ret.add(ns);
    }
    return ret;
  }

  @Override
  public void nodeCreated(String path) {
    if (nsZNode.equals(path)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper", ke);
        watcher.abort("Zookeeper error obtaining namespace node children", ke);
      } catch (IOException e) {
        LOG.error("Error reading data from zookeeper", e);
        watcher.abort("Zookeeper error obtaining namespace node children", e);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (nsZNode.equals(ZKUtil.getParent(path))) {
      String nsName = ZKUtil.getNodeName(path);
      cache.remove(nsName);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (nsZNode.equals(ZKUtil.getParent(path))) {
      // update cache on an existing table node
      String table = ZKUtil.getNodeName(path);
      try {
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        NamespaceDescriptor ns =
            ProtobufUtil.toNamespaceDescriptor(
                NamespaceProtos.NamespaceDescriptor.parseFrom(data));
        cache.put(ns.getName(), ns);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for node "+table, ke);
        // only option is to abort
        watcher.abort("Zookeeper error getting data for node " + table, ke);
      } catch (IOException ioe) {
        LOG.error("Error deserializing namespace: "+path, ioe);
        watcher.abort("Error deserializing namespace: "+path, ioe);
      }
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (nsZNode.equals(path)) {
      // table permissions changed
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, nsZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Error reading data from zookeeper for path "+path, ke);
        watcher.abort("Zookeeper error get node children for path "+path, ke);
      } catch (IOException e) {
        LOG.error("Error deserializing namespace child from: "+path, e);
        watcher.abort("Error deserializing namespace child from: " + path, e);
      }
    }
  }

  private void deleteNamespace(String name) throws IOException {
    String zNode = ZKUtil.joinZNode(nsZNode, name);
    try {
      ZKUtil.deleteNode(watcher, zNode);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for namespace "+name, e);
      throw new IOException("Failed updating permissions for namespace "+name, e);
    }
  }

  private void writeNamespace(NamespaceDescriptor ns) throws IOException {
    String zNode = ZKUtil.joinZNode(nsZNode, ns.getName());
    try {
      ZKUtil.createWithParents(watcher, zNode);
      ZKUtil.updateExistingNodeData(watcher, zNode,
          ProtobufUtil.toProtoBuf(ns).toByteArray(), -1);
    } catch (KeeperException e) {
      LOG.error("Failed updating permissions for namespace "+ns.getName(), e);
      throw new IOException("Failed updating permissions for namespace "+ns.getName(), e);
    }
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) throws IOException {
    for (ZKUtil.NodeAndData n : nodes) {
      if (n.isEmpty()) continue;
      String path = n.getNode();
      String namespace = ZKUtil.getNodeName(path);
      byte[] nodeData = n.getData();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating namespace cache from node "+namespace+" with data: "+
            Bytes.toStringBinary(nodeData));
      }
      NamespaceDescriptor ns =
          ProtobufUtil.toNamespaceDescriptor(
              NamespaceProtos.NamespaceDescriptor.parseFrom(nodeData));
      cache.put(ns.getName(), ns);
    }
  }
}
