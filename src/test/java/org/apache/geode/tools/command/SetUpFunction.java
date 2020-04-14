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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;

/**
 * Function used to set up the distributed sequence tool.
 */
class SetUpFunction implements Function<List<SetUpFunction.GatewayConfiguration>> {
  public static final String FUNCTION_ID = "DistributedCommandSetUp";
  public static final String DISTRIBUTED_COMMAND_REGION = "distributedCommand";
  public static final String DISTRIBUTED_GATEWAY_COMMAND_NAME = "senderToSite_";

  private final static Logger logger = LogService.getLogger();
  private static final ReentrantLock reentrantLock = new ReentrantLock();

  @Override
  public String getId() {
    return FUNCTION_ID;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName) {
    return Collections.singletonList(ResourcePermissions.DATA_WRITE);
  }

  @Override
  public Collection<ResourcePermission> getRequiredPermissions(String regionName, Object args) {
    return getRequiredPermissions(regionName);
  }

  /**
   * Creates the gateway receiver, if there's none created locally already.
   * @param internalCache The Geode cache.
   */
  void getOrCreateGatewayReceiver(InternalCache internalCache) {
    if (internalCache.getGatewayReceivers().isEmpty()) {
      GatewayReceiverFactory receiverFactory = internalCache.createGatewayReceiverFactory();
      receiverFactory.setManualStart(false);
      receiverFactory.create();
    }
  }

  /**
   * Gets or creates the gateway-senders required by the distributed command region.
   *
   * @param internalCache The Geode cache.
   * @param gatewayConfigurations The configuration of the gateways to create/retrieve.
   * @return The list of configured gateway-senders, ready for use.
   */
  Set<String> getOrCreateGatewaySenders(InternalCache internalCache, List<GatewayConfiguration> gatewayConfigurations) {
    Set<String> gatewaySenderIds = new HashSet<>();

    // Create GatewaySenders
    GatewaySenderFactory gatewaySenderFactory = internalCache.createGatewaySenderFactory();
    gatewaySenderFactory.setBatchSize(1);
    gatewaySenderFactory.setParallel(false);
    gatewaySenderFactory.setOrderPolicy(GatewaySender.OrderPolicy.KEY);

    for (GatewayConfiguration gatewayConfiguration : gatewayConfigurations) {
      String gatewaySenderId = DISTRIBUTED_GATEWAY_COMMAND_NAME + gatewayConfiguration.remoteDistributedSystemId;
      logger.info("Creating {}...", gatewaySenderId);

      if (internalCache.getGatewaySender(gatewaySenderId) != null) {
        logger.info("Creating {}... Ignored, it already exists!.", gatewaySenderId);
      } else {
        gatewaySenderFactory.create(gatewaySenderId, gatewayConfiguration.remoteDistributedSystemId);
        logger.info("Creating {}... Done!.", gatewaySenderId);
      }

      gatewaySenderIds.add(gatewaySenderId);
    }

    return gatewaySenderIds;
  }

  /**
   * Creates the internal region used to distribute commands.
   *
   * @param internalCache The Geode cache.
   */
  @SuppressWarnings("unchecked")
  void createRegionAndGateways(InternalCache internalCache, List<GatewayConfiguration> gatewayConfigurations) {
    // Check again while holding the lock.
    if (internalCache.getRegion(DISTRIBUTED_COMMAND_REGION) == null) {
      getOrCreateGatewayReceiver(internalCache);
      Set<String> gatewaySenderIds = getOrCreateGatewaySenders(internalCache, gatewayConfigurations);

      // Set Up Region Attributes
      RegionAttributesCreation regionAttributesCreation = new RegionAttributesCreation();
      regionAttributesCreation.setDataPolicy(DataPolicy.EMPTY);
      regionAttributesCreation.setScope(Scope.DISTRIBUTED_ACK);
      regionAttributesCreation.initGatewaySenders(gatewaySenderIds);
      regionAttributesCreation.setKeyConstraint(String.class);
      regionAttributesCreation.setValueConstraint(DistributedCommand.class);
      regionAttributesCreation.setCacheWriter(new DistributedCommandCacheWriter());
      regionAttributesCreation.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      InternalRegionArguments internalRegionArguments = new InternalRegionArguments().setInternalRegion(true);

      try {
        // InternalCacheForClientAccess to allow creating an internal region from client side.
        InternalCacheForClientAccess cacheForClientAccess = internalCache.getCacheForProcessingClientRequests();
        cacheForClientAccess.createInternalRegion(DISTRIBUTED_COMMAND_REGION, regionAttributesCreation, internalRegionArguments);
      } catch (IOException | ClassNotFoundException exception) {
        throw new FunctionException("Internal error while creating the region", exception);
      }
    }
  }

  @Override
  public void execute(FunctionContext<List<GatewayConfiguration>> context) {
    if (!(context.getCache() instanceof InternalCache)) {
      throw new FunctionException("The function needs an instance of InternalCache to execute some non-public methods.");
    }

    List<GatewayConfiguration> gatewayConfigurationList = context.getArguments();
    if (gatewayConfigurationList.isEmpty() || gatewayConfigurationList.stream().anyMatch(gatewayConfiguration -> gatewayConfiguration.remoteDistributedSystemId == null)) {
      throw new FunctionException("GatewaySenders received are not correctly configured.");
    }

    // Create region and gateways if not already created.
    InternalCache internalCache = (InternalCache) context.getCache();
    if (internalCache.getRegion(DISTRIBUTED_COMMAND_REGION) == null) {
      // Hold the lock to avoid multiple threads creating the region and/or diskStore at the same time.
      reentrantLock.lock();

      try {
        createRegionAndGateways(internalCache, gatewayConfigurationList);
      } finally {
        reentrantLock.unlock();
      }
    }
  }

  static class GatewayConfiguration implements DataSerializable {
    Integer remoteDistributedSystemId;

    public GatewayConfiguration(int remoteDistributedSystemId) {
      this.remoteDistributedSystemId = remoteDistributedSystemId;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(remoteDistributedSystemId, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      this.remoteDistributedSystemId = DataSerializer.readInteger(in);
    }
  }
}
