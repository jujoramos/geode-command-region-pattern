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

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A simple {@link CacheWriter} to execute the {@link DistributedCommand}.
 */
public class DistributedCommandCacheWriter extends CacheWriterAdapter<String, DistributedCommand> implements Declarable {
  private final static transient Logger logger = LogService.getLogger();

  @Override
  public void initialize(Cache cache, Properties properties) {
  }

  @Override
  public void beforeCreate(EntryEvent<String, DistributedCommand> event) throws CacheWriterException {
    DistributedCommand distributedCommand = event.getNewValue();
    logger.info("Executing distributed command {}...", distributedCommand.getName());
    distributedCommand.execute();
    logger.info("Executing distributed command {}... Done!.", distributedCommand.getName());
  }
}
