/**
 *
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

package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class to help with dealing with a snapshot description on the client side.
 * There is a corresponding class on the server side.
 */
@InterfaceAudience.Private
public class ClientSnapshotDescriptionUtils {
  /**
   * Check to make sure that the description of the snapshot requested is valid
   * @param snapshot description of the snapshot
   * @throws IllegalArgumentException if the name of the snapshot or the name of the table to
   *           snapshot are not valid names.
   */
  public static void assertSnapshotRequestIsValid(HBaseProtos.SnapshotDescription snapshot)
      throws IllegalArgumentException {
    // make sure the snapshot name is valid
    TableName.isLegalTableQualifierName(Bytes.toBytes(snapshot.getName()));
    if(snapshot.hasTable()) {
      // make sure the table name is valid
      TableName.isLegalNamespaceName(snapshot.getTable().getNamespace().toByteArray());
      TableName.isLegalTableQualifierName(snapshot.getTable().getTableQualifier().toByteArray());
      // FIXME these method names is really bad - trunk will probably change
      // .META. and -ROOT- snapshots are not allowed
      if (HTableDescriptor.isSystemTable(ProtobufUtil.fromProtoBuf(snapshot.getTable()))) {
        throw new IllegalArgumentException("System table snapshots are not allowed");
      }
    }
  }

  /**
   * Returns a single line (no \n) representation of snapshot metadata.  Use this instead of
   * {@link org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription#toString()}.  We don't replace SnapshotDescrpition's toString
   * because it is auto-generated by protoc.
   * @param ssd
   * @return Single line string with a summary of the snapshot parameters
   */
  public static String toString(HBaseProtos.SnapshotDescription ssd) {
    if (ssd == null) {
      return null;
    }
    return "{ ss=" + ssd.getName() +
           " table=" + (ssd.hasTable()?ProtobufUtil.fromProtoBuf(ssd.getTable()):"") +
           " type=" + ssd.getType() + " }";
  }
}
