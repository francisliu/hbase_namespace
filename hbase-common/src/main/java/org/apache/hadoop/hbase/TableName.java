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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Immutable POJO class for representing a fully-qualified table name.
 * Which is of the form:
 * &lt;table namespace&gt;.&lt;table qualifier&gt;
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class TableName implements Comparable<TableName> {

  /** Namespace delimiter */
  //this should always be only 1 byte long
  public final static char NAMESPACE_DELIM = '.';
  // A non-capture group so that this can be embedded.
  // regex is a bit more complicated to support nuance of tables
  // in default namespace
  public static final String VALID_USER_TABLE_REGEX =
      "(?:(?:(?:[a-zA-Z_0-9][a-zA-Z_0-9-]*\\.+)+(?:[a-zA-Z_0-9-]+\\.*))|" +
         "(?:(?:[a-zA-Z_0-9][a-zA-Z_0-9-]*\\.*)))";

  private byte[] name;
  private String nameAsString;
  private byte[] namespace;
  private String namespaceAsString;
  private byte[] qualifier;
  private String qualifierAsString;

  private TableName() {}

  /**
   * Check passed byte buffer, "tableName", is legal user-space table name.
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException If passed <code>tableName</code> is null
   * @throws IllegalArgumentException if passed a tableName
   * that is made of other than 'word' characters or underscores: i.e.
   * <code>[a-zA-Z_0-9.]</code>. '.' are used to delimit the namespace
   * from the table name. A namespace name can contain '.' though it is
   * not recommended and left valid for backwards compatibility.
   *
   * Valid fully qualified table names:
   * foo.bar, namespace=>foo, table=>bar
   * org.foo.bar, namespace=org.foo, table=>bar
   */
  public static byte [] isLegalFullyQualifiedTableName(final byte[] tableName) {
    if (tableName == null || tableName.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    int namespaceDelimIndex = com.google.common.primitives.Bytes.lastIndexOf(tableName,
        (byte) NAMESPACE_DELIM);
    if (namespaceDelimIndex == 0 || namespaceDelimIndex == -1){
      isLegalNamespaceName(tableName);
      isLegalTableQualifierName(tableName);
    } else {
      isLegalNamespaceName(tableName, 0, namespaceDelimIndex);
      isLegalTableQualifierName(tableName, namespaceDelimIndex + 1, tableName.length);
    }
    return tableName;
  }

  private static void isLegalTableQualifierName(final byte[] qualifierName){
    isLegalTableQualifierName(qualifierName, 0, qualifierName.length);
  }

  private static void isLegalTableQualifierName(final byte[] qualifierName,
                                                int offset,
                                                int length){
    boolean foundDot = false;
    for (int i = offset; i < length; i++) {
      if ((Character.isLetterOrDigit(qualifierName[i]) || qualifierName[i] == '_' ||
           qualifierName[i] == '-') && !foundDot) {
        continue;
      }
      if (qualifierName[i] == '.') {
        foundDot = true;
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + qualifierName[i] +
        "> at " + i + ". User-space table qualifiers can only contain " +
        "'alphanumeric characters': i.e. [a-zA-Z_0-9-]: " +
          Bytes.toString(qualifierName, offset, length));
    }
  }

  public static void isLegalNamespaceName(byte[] namespaceName) {
    isLegalNamespaceName(namespaceName, 0, namespaceName.length);
  }

  /**
   * Valid namespace characters are [a-zA-Z_0-9-.]
   * Namespaces cannot start with the characters '.' and '-'.
   * @param namespaceName
   * @param offset
   * @param length
   */
  public static void isLegalNamespaceName(byte[] namespaceName, int offset, int length) {
    if (namespaceName[offset] == '.' || namespaceName[offset] == '-') {
      throw new IllegalArgumentException("Illegal first character <" + namespaceName[0] +
          "> at 0. Namespaces can only start with alphanumeric " +
          "characters': i.e. [a-zA-Z_0-9]: " + Bytes.toString(namespaceName));
    }
    for (int i = offset; i < length; i++) {
      if (Character.isLetterOrDigit(namespaceName[i])|| namespaceName[i] == '_' ||
          namespaceName[i] == '-' ||
          namespaceName[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + namespaceName[i] +
        "> at " + i + ". Namespaces can only contain " +
        "'alphanumeric characters': i.e. [a-zA-Z_0-9-.]: " + Bytes.toString(namespaceName,
          offset, length));
    }
  }

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

    isLegalNamespaceName(namespace);
    isLegalTableQualifierName(qualifier);

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

    isLegalNamespaceName(ret.namespace);
    isLegalTableQualifierName(ret.qualifier);

    ret.nameAsString = createFullyQualified(ret.namespaceAsString, ret.qualifierAsString);
    ret.name = Bytes.toBytes(ret.nameAsString);
    return ret;
  }

  public static TableName valueOf(byte[] name) {
    return valueOf(Bytes.toString(name));
  }

  public static TableName valueOf(String name) {
    //TODO we should need to convert it to bytes again
    isLegalFullyQualifiedTableName(Bytes.toBytes(name));
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
      return TableName.valueOf(name.substring(0, index), name.substring(index + 1));
    }
    return TableName.valueOf(NamespaceDescriptor.DEFAULT_NAMESPACE.getName(), name);
  }

  private static String createFullyQualified(String namespace, String tableQualifier) {
    if (namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE.getName())) {
      return tableQualifier;
    }
    return namespace+NAMESPACE_DELIM+tableQualifier;
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
