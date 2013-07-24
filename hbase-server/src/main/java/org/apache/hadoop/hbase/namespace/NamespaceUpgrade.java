/**
 * The Apache Software Foundation
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
package org.apache.hadoop.hbase.namespace;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.IOException;
import java.util.List;

/**
 * Upgrades old 0.94 filesystem layout to namespace layout
 * Does the following:
 *
 * - creates system namespace directory and move META table there
 * renaming META table to hbase.meta,
 * this in turn would require to re-encode the region directory name
 * - creates default namespace directory, all tables without '.' in them will be moved here
 * - For each table that contains a '.', parse it as a fully-qualified table name
 * and automatically create the appropriate namespace and move the respective table
 * - During startup TableNamespaceManager, will populate the namespace table
 * with the appropriate namespace descriptors
 */
public class NamespaceUpgrade {
  private static final Log LOG = LogFactory.getLog(NamespaceUpgrade.class);

  public void upgradeTableDirs(Configuration conf, Path rootDir) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path newMetaRegionDir = HRegion.getRegionDir(rootDir, HRegionInfo.FIRST_META_REGIONINFO);
    //if new meta region exists then migration was completed successfully
    if (!fs.exists(newMetaRegionDir) && fs.exists(rootDir)) {
      Path sysNsDir = FSUtils.getNamespaceDir(rootDir, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
      Path defNsDir = FSUtils.getNamespaceDir(rootDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
      if (!fs.exists(sysNsDir)) {
        if (!fs.mkdirs(sysNsDir)) {
          throw new IOException("Failed to create system namespace dir: "+sysNsDir);
        }
      }
      if (!fs.exists(defNsDir)) {
        if (!fs.mkdirs(defNsDir)) {
          throw new IOException("Failed to create default namespace dir: "+defNsDir);
        }
      }

      List<String> sysTables = Lists.newArrayList("-ROOT-",".META.");

      //migrate tables including archive and tmp
      Path baseDirs[] = {rootDir,
          new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
          new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY)};
      for(Path baseDir: baseDirs) {
        List<Path> oldTableDirs = FSUtils.getLocalTableDirs(fs, baseDir);
        for(Path oldTableDir: oldTableDirs) {
          if (!sysTables.contains(oldTableDir.getName())) {
            Path nsDir = FSUtils.getTableDir(baseDir,
                TableName.valueOf(oldTableDir.getName()));
            if(!fs.exists(nsDir.getParent())) {
              if(!fs.mkdirs(nsDir.getParent())) {
                throw new IOException("Failed to create namespace dir "+nsDir.getParent());
              }
            }
            if (sysTables.indexOf(oldTableDir.getName()) < 0) {
              LOG.info("Migrating table " + oldTableDir.getName() + " to " + nsDir);
              if (!fs.rename(oldTableDir, nsDir)) {
                throw new IOException("Failed to move "+oldTableDir+" to namespace dir "+nsDir);
              }
            }
          }
        }
      }

      //migrate snapshot dir
      Path oldSnapshotDir = new Path(rootDir, HConstants.OLD_SNAPSHOT_DIR_NAME);
      Path newSnapshotDir = new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
      if (fs.exists(oldSnapshotDir)) {
        boolean foundOldSnapshotDir = false;
        // Logic to verify old snapshot dir culled from SnapshotManager
        // ignore all the snapshots in progress
        FileStatus[] snapshots = fs.listStatus(oldSnapshotDir,
          new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
        // loop through all the completed snapshots
        for (FileStatus snapshot : snapshots) {
          Path info = new Path(snapshot.getPath(), SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
          // if the snapshot is bad
          if (fs.exists(info)) {
            foundOldSnapshotDir = true;
            break;
          }
        }
        if(foundOldSnapshotDir) {
          LOG.info("Migrating snapshot dir");
          if (!fs.rename(oldSnapshotDir, newSnapshotDir)) {
            throw new IOException("Failed to move old snapshot dir "+
                oldSnapshotDir+" to new "+newSnapshotDir);
          }
        }
      }

      Path newMetaDir = FSUtils.getTableDir(rootDir, HConstants.META_TABLE_NAME);
      Path oldMetaDir = new Path(rootDir, ".META.");
      if (fs.exists(oldMetaDir)) {
        LOG.info("Migrating meta table " + oldMetaDir.getName() + " to " + newMetaDir);
        if (!fs.rename(oldMetaDir, newMetaDir)) {
          throw new IOException("Failed to migrate meta table "
              + oldMetaDir.getName() + " to " + newMetaDir);
        }
      }

      //since meta table name has changed
      //rename meta region dir from it's old encoding to new one
      Path oldMetaRegionDir = HRegion.getRegionDir(rootDir,
          new Path(newMetaDir, "1028785192").toString());
      if (fs.exists(oldMetaRegionDir)) {
        LOG.info("Migrating meta region " + oldMetaRegionDir + " to " + newMetaRegionDir);
        if (!fs.rename(oldMetaRegionDir, newMetaRegionDir)) {
          throw new IOException("Failed to migrate meta region "
              + oldMetaRegionDir + " to " + newMetaRegionDir);
        }
      }
    }
  }
}
