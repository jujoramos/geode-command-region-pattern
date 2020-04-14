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
package org.apache.geode.tools.command.statsMgmt;

import java.io.Serializable;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.tools.command.DistributedCommand;

/**
 *
 */
public class IncrementStatisticCommand implements DistributedCommand, Serializable {
  final static String STATISTIC_TYPE_ID = "DummyStats";
  final static String INVOCATION_COUNT_STATISTIC_ID = "invocationCount";

  @Override
  public void execute() {
    StatisticsFactory statisticsFactory = CacheFactory.getAnyInstance().getDistributedSystem();
    Statistics myStatistic = statisticsFactory.findStatisticsByTextId(STATISTIC_TYPE_ID)[0];
    myStatistic.incLong(INVOCATION_COUNT_STATISTIC_ID, 1L);
  }
}
