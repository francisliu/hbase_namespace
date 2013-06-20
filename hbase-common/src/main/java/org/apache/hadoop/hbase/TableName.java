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

package org.apache.hadoop.hbase;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Immutable POJO class for representing a fully-qualified table name.
 * Which is of the form:
 * &lt;table namespace&gt;.&lt;table qualifier&gt;
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class TableName implements Comparable<TableName> {
  public static final Log LOG = LogFactory.getLog(TableName.class);

  /** Namespace delimiter */
  //this should always be only 1 byte long
  public final static char NAMESPACE_DELIM = '.';

  private static Set<String> exceptionTables = new HashSet<String>();
  private static Set<String> exceptionNS = new HashSet<String>();
  private static boolean exceptionInitialized = false;

  private byte[] name;
  private String nameAsString;
  private byte[] namespace;
  private String namespaceAsString;
  private byte[] qualifier;
  private String qualifierAsString;

  private TableName() {}

  public byte[] getName() {
    return name;
  }

  public String getNameAsString() {
    return nameAsString;
  }

  public byte[] getNamespace() {
    return namespace;
  }

  public String getNamespaceAsString() {
    return namespaceAsString;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public String getQualifierAsString() {
    return qualifierAsString;
  }

  public byte[] toBytes() {
    return name;
  }

  @Override
  public String toString() {
    return nameAsString;
  }

  public static TableName valueOf(byte[] namespace, byte[] qualifier) {
    TableName ret = new TableName();
    ret.namespace = namespace;
    ret.namespaceAsString = Bytes.toString(namespace);
    ret.qualifier = qualifier;
    ret.qualifierAsString = Bytes.toString(qualifier);
    ret.nameAsString = createFullyQualified(ret.namespaceAsString, ret.qualifierAsString);
    ret.name = Bytes.toBytes(ret.nameAsString);
    return ret;
  }

  public static TableName valueOf(String namespaceAsString, String qualifierAsString) {
    TableName ret = new TableName();
    ret.namespaceAsString = namespaceAsString;
    ret.qualifier = Bytes.toBytes(qualifierAsString);
    ret.qualifierAsString = qualifierAsString;
    ret.nameAsString = createFullyQualified(ret.namespaceAsString, ret.qualifierAsString);
    ret.name = Bytes.toBytes(ret.nameAsString);
    return ret;
  }

  public static TableName valueOf(byte[] name) {
    return valueOf(Bytes.toString(name));
  }

  public static TableName valueOf(String name) {
    if(!exceptionInitialized) {
      throw new IllegalStateException("refereshExceptionTables must be called prior to using this" +
          " method");
    }
    if(containsExceptionTable(name)) {
      return TableName.valueOf(NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), name);
    }
    int index = -1;
    for(int i=1;i<name.length();i++) {
      if (name.charAt(i) == NAMESPACE_DELIM && index == -1) {
        index = i;
      }
      if (name.charAt(i) != NAMESPACE_DELIM &&
         name.charAt(i-1) == NAMESPACE_DELIM) {
        index = i-1;
      }
    }
    if (index != -1) {
      return TableName.valueOf(name.substring(0,index), name.substring(index+1));
    }
    return TableName.valueOf(NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), name);
  }

  private static String createFullyQualified(String namespace, String tableQualifier) {
    if (namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE.getName())) {
      return tableQualifier;
    }
    return namespace+ NAMESPACE_DELIM+tableQualifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableName tableName = (TableName) o;

    if (!nameAsString.equals(tableName.nameAsString)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = nameAsString.hashCode();
    return result;
  }

  @Override
  public int compareTo(TableName tableName) {
    return this.nameAsString.compareTo(tableName.getNameAsString());
  }

  @InterfaceAudience.Private
  public static boolean containsExceptionTable(String tableName) {
    return exceptionTables.contains(tableName);
  }

  @InterfaceAudience.Private
  public static boolean containsExceptionNS(String namespace) {
    return exceptionNS.contains(namespace);
  }

  /**
   * One of the refresh apis need to be called prior to threads get started
   */
  @InterfaceAudience.Private
  public static void refereshExceptionTables() {
    refereshExceptionTables(HBaseConfiguration.create());
  }

  /**
   * One of the refresh apis need to be called prior to threads get started
   */
  @InterfaceAudience.Private
  public static synchronized void refereshExceptionTables(Configuration conf) {
    try {
      Set<String> exceptionTables = new HashSet<String>();
      Set<String> exceptionNS = new HashSet<String>();
      Class<?> clazz = Class.forName("org.apache.hadoop.hbase.util.FSUtils");
      Method m;
      m = clazz.getMethod("getRootDir", Configuration.class);
      Path hbaseDir = (Path)m.invoke(null, conf);
      Path[] baseDirs = {hbaseDir,
          new Path(hbaseDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
          new Path(hbaseDir, HConstants.HBASE_TEMP_DIRECTORY)};
      for(Path baseDir: baseDirs) {
        m = clazz.getMethod("getNamespaceDir", Path.class, String.class);
        Path nsDir = (Path) m.invoke(null, baseDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
        m = clazz.getMethod("getLocalTableDirs", FileSystem.class, Path.class);
        List<Path> dirs = (List<Path>) m.invoke(null, FileSystem.get(conf), nsDir);
        for(Path dir: dirs) {
          TableName tableName = TableName.valueOf(dir.getName());
          if(!tableName.getNamespaceAsString()
              .equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
            exceptionTables.add(tableName.toString());
            exceptionNS.add(tableName.getNamespaceAsString());
          }
        }
      }
      LOG.debug("Loaded exception tables: "+exceptionTables);
      LOG.debug("Loaded exception namespaces: "+exceptionNS);
      TableName.exceptionNS = exceptionNS;
      TableName.exceptionTables = exceptionTables;
      exceptionInitialized = true;
    } catch (ClassNotFoundException e) {
    } catch (NoSuchMethodException e) {
      LOG.error("Failed to refereshExceptionTables, this api can only be called by hbase", e);
      throw new IllegalStateException("Failed to refereshExceptionTables, "+
          "this api is internal.", e);
    } catch (InvocationTargetException e) {
      LOG.error("Failed to refereshExceptionTables, this api can only be called by hbase", e);
      throw new IllegalStateException("Failed to refereshExceptionTables, "+
          "this api is internal.", e);
    } catch (IllegalAccessException e) {
      LOG.error("Failed to refereshExceptionTables, this api can only be called by hbase", e);
      throw new IllegalStateException("Failed to refereshExceptionTables, "+
          "this api is internal.", e);
    } catch (IOException e) {
      LOG.error("Failed to refereshExceptionTables, this api can only be called by hbase", e);
      throw new IllegalStateException("Failed to refereshExceptionTables, "+
          "this api is internal.", e);
    }
  }
}
