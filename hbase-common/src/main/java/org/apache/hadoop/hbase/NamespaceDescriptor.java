/**
 * Copyright The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under
 * one or more contributor license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class NamespaceDescriptor {

  public static NamespaceDescriptor DEFAULT_NAMESPACE
      = NamespaceDescriptor.create(HConstants.DEFAULT_NAMESPACE_NAME_STR).build();
  public static NamespaceDescriptor SYSTEM_NAMESPACE
      = NamespaceDescriptor.create(HConstants.SYSTEM_NAMESPACE_NAME_STR).build();
  public static String NAMESPACE_DELIM = ".";

  private String name;
  private Map<byte[], byte[]> values;

  public static final Comparator<NamespaceDescriptor>
      NAMESPACE_DESCRIPTOR_COMPARATOR = new Comparator<NamespaceDescriptor>() {
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
    return values.get(key);
  }

  public String getValue(String key) {
    return Bytes.toString(values.get(Bytes.toBytes(key)));
  }

  public Map<byte[], byte[]> getValues() {
    return Collections.unmodifiableMap(values);
  }

  public static Path getNamespaceDir(Path rootdir, final String namespace) {
    //TODO nshack to avoid migrating existing tables
    if(namespace.equals(HConstants.SYSTEM_NAMESPACE_NAME_STR)) {
      return rootdir;
    } else {
      return new Path(rootdir, new Path(HConstants.BASE_NAMESPACE_DIR,
          new Path(namespace)));
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
    for (Map.Entry<byte[], byte[]> e:
        values.entrySet()) {
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
    private Map<byte[], byte[]> bValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

    private Builder(NamespaceDescriptor ns) {
      this.bName = ns.name;
      this.bValues = ns.values;
    }

    private Builder(String name) {
      this.bName = name;
    }
    
    public Builder addValues(Map<byte[], byte[]> values) {
      this.bValues.putAll(values);
      return this;
    }

    public Builder addValue(byte[] key, byte[] value) {
      this.bValues.put(key, value);
      return this;
    }

    public Builder addValue(String key, String value) {
      this.bValues.put(Bytes.toBytes(key), Bytes.toBytes(value));
      return this;
    }

    public Builder removeValue(String key) {
      this.bValues.remove(Bytes.toBytes(key));
      return this;
    }

    public Builder removeValue(byte[] key) {
      this.bValues.remove(key);
      return this;
    }

    public NamespaceDescriptor build() {
      if(this.bName == null){
         throw new IllegalArgumentException("A name has to be specified in a namespace.");
      }
      
      NamespaceDescriptor desc = new NamespaceDescriptor(this.bName);
      desc.values = this.bValues;
      return desc;
    }
  }
}
