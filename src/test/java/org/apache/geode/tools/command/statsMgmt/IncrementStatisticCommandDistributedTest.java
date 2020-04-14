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

import static java.util.Arrays.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.tools.command.statsMgmt.IncrementStatisticCommand.INVOCATION_COUNT_STATISTIC_ID;
import static org.apache.geode.tools.command.statsMgmt.IncrementStatisticCommand.STATISTIC_TYPE_ID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.Region;
import org.apache.geode.tools.command.AbstractDistributedCommandTest;
import org.apache.geode.tools.command.DistributedCommand;

/**
 *
 */
public class IncrementStatisticCommandDistributedTest extends AbstractDistributedCommandTest {

  private Long getInvocationCount() {
    StatisticsFactory statisticsFactory = cacheRule.getCache().getDistributedSystem();
    Statistics myStatistic = statisticsFactory.findStatisticsByTextId(STATISTIC_TYPE_ID)[0];
    assertThat(myStatistic).isNotNull();

    return (Long) myStatistic.get(INVOCATION_COUNT_STATISTIC_ID);
  }

  @Test
  public void incrementStatisticCommandTest() {
    asList(cluster1Server1, cluster1Server2, cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      StatisticsFactory statisticsFactory = cacheRule.getCache().getDistributedSystem();
      StatisticsType statisticsType = statisticsFactory.createType(STATISTIC_TYPE_ID, "Stats for my dummy command.",
          new StatisticDescriptor[] {
              statisticsFactory.createLongCounter(INVOCATION_COUNT_STATISTIC_ID, "Total amount of invocations.", "operations")
          }
      );

      statisticsFactory.createStatistics(statisticsType, STATISTIC_TYPE_ID);
    }));

    // Assert invocationCount is zero in all members.
    asList(cluster1Server1, cluster1Server2, cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      await().untilAsserted(() -> assertThat(getInvocationCount()).isEqualTo(0L));
    }));

    // Create the IncrementStatisticCommand
    IncrementStatisticCommand incrementStatisticCommand = new IncrementStatisticCommand();

    // Put the command into Cluster1 5 times
    cluster1Server1.invoke(() -> {
      Region<String, DistributedCommand> region = cacheRule.getCache().getRegion(getDistributedCommandRegionName());
      IntStream.range(0, 5).forEach(value -> region.put("IncrementStatistic_" + value, incrementStatisticCommand));
      assertThat(getInvocationCount()).isEqualTo(5L);
    });

    // Wait for queues to be empty (aka events dispatched to remote cluster)
    waitForEmptySenderQueues(asList(cluster1Server1, cluster1Server2));

    // Assert InvocationCount is 5 on remote cluster.
    Long invocationCount1 = cluster2Server1.invoke(this::getInvocationCount);
    Long invocationCount2 = cluster2Server2.invoke(this::getInvocationCount);
    assertThat(invocationCount1 + invocationCount2).isEqualTo(5L);
  }
}
