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
package org.apache.geode.tools.command.clear;

import static java.util.Arrays.asList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.tools.command.AbstractDistributedCommandTest;
import org.apache.geode.tools.command.DistributedCommand;

/**
 *
 */
public class ClearRegionCommandDistributedTest extends AbstractDistributedCommandTest {

  @Test
  public void clearRegionCommandTest() {
    final int entries = 100;
    final String regionName = testName.getMethodName();

    // Create ReplicateRegion in all servers.
    asList(cluster1Server1, cluster1Server2, cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    }));

    // Populate Region on both clusters.
    asList(cluster1Server1, cluster2Server1).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      IntStream.range(0, entries).forEach(i -> region.put("Key" + i, "Value" + i));
    }));

    // Assert Region Size on all members.
    asList(cluster1Server1, cluster1Server2, cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(entries);
    }));

    // Create the ClearRegionCommand
    ClearRegionCommand clearRegionCommand = new ClearRegionCommand(regionName);

    // Put the command into Cluster1 server
    cluster1Server1.invoke(() -> {
      Region<String, DistributedCommand> region = cacheRule.getCache().getRegion(getDistributedCommandRegionName());
      region.put("ClearCommand1", clearRegionCommand);
    });

    // Assert region is empty on all members.
    asList(cluster1Server1, cluster1Server2, cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      await().untilAsserted(() -> assertThat(region.isEmpty()).isTrue());
    }));
  }
}
