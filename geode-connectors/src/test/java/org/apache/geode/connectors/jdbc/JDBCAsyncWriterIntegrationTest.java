/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.awaitility.Awaitility;

@Category(IntegrationTest.class)
public class JDBCAsyncWriterIntegrationTest {

  private Cache cache;

  private Connection conn;

  private Statement stmt;

  JDBCAsyncWriter jdbcWriter;

  private String dbName="DerbyDB";

  private String regionTableName = "employees";

  @Before
  public void setup() throws Exception {
    try {
      cache = CacheFactory.getAnyInstance();
    } catch (Exception e) {
      // ignore
    }
    if (null == cache) {
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
    }
    setupDB();
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache = null;
    }
    closeDB();
  }

  public void setupDB() throws Exception {
    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    String connectionURL = "jdbc:derby:memory:" + dbName + ";create=true";
    Class.forName(driver);
    conn = DriverManager.getConnection(connectionURL);
    stmt = conn.createStatement();
    stmt.execute("Create Table " + regionTableName + " (id varchar(10), name varchar(10))");
  }

  public void closeDB() throws Exception {
    if (stmt == null) {
      stmt = conn.createStatement();
    }
    stmt.execute("Drop table " + regionTableName);
    stmt.close();

    if (conn != null) {
      conn.close();
    }
  }

  @Test
  public void canInstallJDBCAsyncWriterOnRegion() {
    Region employees = createRegionWithJDBCAsyncWriter("employees");
    employees.put("1", "Emp1");
    employees.put("2", "Emp2");

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
    .until(() -> assertThat(jdbcWriter.getTotalEvents()).isEqualTo(2));

  }

  @Test
  public void jdbcAsyncWriterCanWriteToDatabase() throws Exception {
    Region employees = createRegionWithJDBCAsyncWriter("employees");

    employees.put("1", "Emp1");
    employees.put("2", "Emp2");

    Awaitility.await().atMost(30, TimeUnit.SECONDS)
    .until(() -> assertThat(jdbcWriter.getSuccessfulEvents()).isEqualTo(2));

    validateTableRowCount(2);
  }

  private Region createRegionWithJDBCAsyncWriter(String regionName) {
    jdbcWriter = new JDBCAsyncWriter();
    cache.createAsyncEventQueueFactory().setBatchSize(1)
    .setBatchTimeInterval(1).create("jdbcAsyncQueue", jdbcWriter);

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.addAsyncEventQueueId("jdbcAsyncQueue");
    return rf.create(regionName);
  }

  private void validateTableRowCount(int expected) throws Exception {
    ResultSet rs = stmt.executeQuery("select count(*) from " + regionTableName);
    rs.next();
    int size = rs.getInt(1);
    assertThat(size).isEqualTo(expected);
  }

}
