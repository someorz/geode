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

import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;

/*
 * This class provides write behind cache semantics for a JDBC data source using AsyncEventListener.
 * 
 * @since Geode 1.4
 */
public class JDBCAsyncWriter implements AsyncEventListener {

  private long totalEvents = 0;

  private long successfulEvents = 0;

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean processEvents(List<AsyncEvent> events) {
    totalEvents += events.size();
    return true;
  }

  @Override
  public void init(Properties props) {

  };

  public long getTotalEvents() {
    return this.totalEvents;
  }

  public long getsuccessfulEvents() {
    return this.successfulEvents;
  }
}
