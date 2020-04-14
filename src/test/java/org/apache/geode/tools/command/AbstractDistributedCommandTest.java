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
package org.apache.geode.tools.command;

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPortsForDUnitSite;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 *
 */
public abstract class AbstractDistributedCommandTest implements Serializable {
  protected VM cluster1Server1, cluster1Server2;
  protected VM cluster2Server1, cluster2Server2;

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  protected String getDistributedCommandRegionName() {
    return SetUpFunction.DISTRIBUTED_COMMAND_REGION;
  }

  protected Properties createLocatorConfig(int systemId, int localLocatorPort, int remoteLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(systemId));
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');
    config.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocatorPort + ']');
    config.setProperty(START_LOCATOR, "localhost[" + localLocatorPort + "],server=true,peer=true,hostname-for-clients=localhost");
    config.setProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.tools.command.**");

    return config;
  }

  protected Properties createServerConfig(int systemId, int localLocatorPort) {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(systemId));
    config.setProperty(LOCATORS, "localhost[" + localLocatorPort + ']');
    config.setProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.tools.command.**");

    return config;
  }

  protected void waitForEmptySenderQueues(List<VM> vms) {
    vms.forEach(vm -> vm.invoke(() -> {
      cacheRule.getCache().getGatewaySenders().forEach(gatewaySender -> {
        Set<RegionQueue> queues = ((AbstractGatewaySender) gatewaySender).getQueues();
        for (RegionQueue queue : queues) {
          await().untilAsserted(() -> assertThat(queue.size()).isEqualTo(0));
        }
      });
    }));
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    final int[] ports = getRandomAvailableTCPPortsForDUnitSite(2);
    int locatorSite1Port = ports[0];
    int locatorSite2Port = ports[1];

    // SetUp Locator in Cluster 1
    VM cluster1Locator1 = getVM(0);
    cluster1Locator1.invoke(() -> {
      Properties config = createLocatorConfig(1, locatorSite1Port, locatorSite2Port);
      cacheRule.createCache(config);
    });

    // SetUp Locator in Cluster 2
    VM cluster2Locator1 = getVM(1);
    cluster2Locator1.invoke(() -> {
      Properties config = createLocatorConfig(2, locatorSite2Port, locatorSite1Port);
      cacheRule.createCache(config);
    });

    // Set Up Servers in Cluster 1
    cluster1Server1 = getVM(2); cluster1Server2 = getVM(3);
    asList(cluster1Server1, cluster1Server2).forEach(vm -> vm.invoke(() -> {
      cacheRule.createCache(createServerConfig(1, locatorSite1Port));
      FunctionService
          .onMember(cacheRule.getCache().getMyId())
          .setArguments(Collections.singletonList(new SetUpFunction.GatewayConfiguration(2)))
          .execute(new SetUpFunction());
    }));

    // Set Up Servers in Cluster 2
    cluster2Server1 = getVM(4); cluster2Server2 = getVM(5);
    asList(cluster2Server1, cluster2Server2).forEach(vm -> vm.invoke(() -> {
      cacheRule.createCache(createServerConfig(2, locatorSite2Port));
      FunctionService
          .onMember(cacheRule.getCache().getMyId())
          .setArguments(Collections.singletonList(new SetUpFunction.GatewayConfiguration(1)))
          .execute(new SetUpFunction());
    }));
  }
}
