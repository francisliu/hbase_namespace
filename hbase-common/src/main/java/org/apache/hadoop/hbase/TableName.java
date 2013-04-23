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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * POJO class for representing a fully-qualified table name.
 * Which is of the form:
 * &lt;table namespace&gt;.&lt;table qualifier&gt;
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableName implements Comparable<TableName> {

  /** Namespace delimiter */
  public static String NAMESPACE_DELIM = ".";

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
    ret.namespace = Bytes.toBytes(namespaceAsString);
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
    List<String> list = Lists.newArrayList(Splitter.on(NAMESPACE_DELIM).limit(2).split(name));
    if (list.size() == 2) {
      return TableName.valueOf(list.get(0), list.get(1));
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
}
