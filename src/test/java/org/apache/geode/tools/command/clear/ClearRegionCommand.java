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

import java.io.Serializable;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.tools.command.DistributedCommand;

/**
 *
 */
public class ClearRegionCommand implements DistributedCommand, Serializable {
  private final static transient Logger logger = LogService.getLogger();
  private final String regionName;

  public ClearRegionCommand(String regionName) {
    this.regionName = regionName;
  }

  @Override
  public void execute() {
    logger.info("Clearing region {}...", regionName);
    CacheFactory.getAnyInstance().getRegion(regionName).clear();
    logger.info("Clearing region {}... Done!.", regionName);
  }
}
