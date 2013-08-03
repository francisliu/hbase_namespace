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

package org.apache.hadoop.hbase.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that FileLink switches between alternate locations
 * when the current location moves or gets deleted.
 */
@Category(MediumTests.class)
public class TestHFileLink {

  @Test
  public void testValidLinkNames() {
    String validLinkNames[] = {"foo=fefefe-0123456", "ns=foo=abababa-fefefefe"};

    for(String name : validLinkNames) {
      Assert.assertTrue("Failed validating:" + name, name.matches(HFileLink.LINK_NAME_REGEX));
    }

    for(String name : validLinkNames) {
      Assert.assertTrue("Failed validating:" + name, HFileLink.isHFileLink(name));
    }

    String testName = "foo=fefefe-0123456";
    Assert.assertEquals(TableName.valueOf("foo"),
        HFileLink.getReferencedTableName(testName));
    Assert.assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    Assert.assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    Assert.assertEquals(testName,
        HFileLink.createHFileLinkName(TableName.valueOf("foo"), "fefefe", "0123456"));

    testName = "ns=foo=fefefe-0123456";
    Assert.assertEquals(TableName.valueOf("ns", "foo"),
        HFileLink.getReferencedTableName(testName));
    Assert.assertEquals("fefefe", HFileLink.getReferencedRegionName(testName));
    Assert.assertEquals("0123456", HFileLink.getReferencedHFileName(testName));
    Assert.assertEquals(testName,
        HFileLink.createHFileLinkName(TableName.valueOf("ns", "foo"), "fefefe", "0123456"));

    for(String name : validLinkNames) {
      Matcher m = HFileLink.LINK_NAME_PATTERN.matcher(name);
      assertTrue(m.matches());
      Assert.assertEquals(HFileLink.getReferencedTableName(name),
          TableName.valueOf(m.group(1), m.group(2)));
      Assert.assertEquals(HFileLink.getReferencedRegionName(name),
          m.group(3));
      Assert.assertEquals(HFileLink.getReferencedHFileName(name),
          m.group(4));
    }
  }

  @Test
  public void testBackReference() {
    assertEquals("region.foo", HFileLink.createBackReferenceName("foo", "region"));
    assertEquals("region.ns=foo", HFileLink.createBackReferenceName("ns:foo", "region"));

    Pair<TableName, String> parsedRef = HFileLink.parseBackReferenceName("region.foo");
    assertEquals(parsedRef.getFirst(), TableName.valueOf("foo"));
    assertEquals(parsedRef.getSecond(), "region");

    parsedRef = HFileLink.parseBackReferenceName("region.ns=foo");
    assertEquals(parsedRef.getFirst(), TableName.valueOf("ns", "foo"));
    assertEquals(parsedRef.getSecond(), "region");
  }


}
