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
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Namespace POJO class. Used to represent and define namespaces.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class NamespaceDescriptor {

  /** System namespace name. */
  public static final byte [] SYSTEM_NAMESPACE_NAME = Bytes.toBytes("hbase");
  public static final String SYSTEM_NAMESPACE_NAME_STR =
      Bytes.toString(SYSTEM_NAMESPACE_NAME);
  /** Default namespace name. */
  public static final byte [] DEFAULT_NAMESPACE_NAME = Bytes.toBytes("default");
  public static final String DEFAULT_NAMESPACE_NAME_STR =
      Bytes.toString(DEFAULT_NAMESPACE_NAME);

  public static final NamespaceDescriptor DEFAULT_NAMESPACE = NamespaceDescriptor.create(
    DEFAULT_NAMESPACE_NAME_STR).build();
  public static final NamespaceDescriptor SYSTEM_NAMESPACE = NamespaceDescriptor.create(
    SYSTEM_NAMESPACE_NAME_STR).build();

  private String name;
  private Map<byte[], byte[]> properties;

  public static final Comparator<NamespaceDescriptor> NAMESPACE_DESCRIPTOR_COMPARATOR =
      new Comparator<NamespaceDescriptor>() {
    @Override
    public int compare(NamespaceDescriptor namespaceDescriptor,
        NamespaceDescriptor namespaceDescriptor2) {
      return namespaceDescriptor.getName().compareTo(namespaceDescriptor2.getName());
    }
  };

  private NamespaceDescriptor() {
  }

  private NamespaceDescriptor(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public byte[] getValue(byte[] key) {
    return properties.get(key);
  }

  public String getValue(String key) {
    return Bytes.toString(properties.get(Bytes.toBytes(key)));
  }

  public Map<byte[], byte[]> getProperties() {
    return Collections.unmodifiableMap(properties);
  }

  public static void isLegalNamespaceName(byte[] namespaceName) {
    isLegalNamespaceName(namespaceName, 0, namespaceName.length);
  }

  public static void isLegalNamespaceName(byte[] namespaceName, int offset, int length) {
    if (Bytes.equals(namespaceName, offset, length,
        SYSTEM_NAMESPACE_NAME, 0, SYSTEM_NAMESPACE_NAME.length)){
      return;
    }
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

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(name);
    s.append("'");
    for (Map.Entry<byte[], byte[]> e : properties.entrySet()) {
      String key = Bytes.toString(e.getKey());
      String value = Bytes.toString(e.getValue());
      if (key == null) {
        continue;
      }
      s.append(", ");
      s.append(key);
      s.append(" => '");
      s.append(value);
      s.append("'");
    }
    s.append('}');
    return s.toString();
  }

  public static Builder create(String name) {
    return new Builder(name);
  }

  public static Builder create(NamespaceDescriptor ns) {
    return new Builder(ns);
  }

  public static class Builder {
    private String bName;
    private Map<byte[], byte[]> bProperties = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

    private Builder(NamespaceDescriptor ns) {
      this.bName = ns.name;
      this.bProperties = ns.properties;
    }

    private Builder(String name) {
      this.bName = name;
    }
    
    public Builder addProperties(Map<byte[], byte[]> values) {
      this.bProperties.putAll(values);
      return this;
    }

    public Builder addProperty(byte[] key, byte[] value) {
      this.bProperties.put(key, value);
      return this;
    }

    public Builder addProperty(String key, String value) {
      this.bProperties.put(Bytes.toBytes(key), Bytes.toBytes(value));
      return this;
    }

    public Builder removeValue(String key) {
      this.bProperties.remove(Bytes.toBytes(key));
      return this;
    }

    public Builder removeValue(byte[] key) {
      this.bProperties.remove(key);
      return this;
    }

    public NamespaceDescriptor build() {
      if (this.bName == null){
         throw new IllegalArgumentException("A name has to be specified in a namespace.");
      }
      
      NamespaceDescriptor desc = new NamespaceDescriptor(this.bName);
      desc.properties = this.bProperties;
      return desc;
    }
  }
}
