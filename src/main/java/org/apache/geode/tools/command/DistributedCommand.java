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

/**
 * The command interface users must implement when designing commands.
 */
public interface DistributedCommand {

  /**
   * Executes the command.
   */
  void execute();

  /**
   * Returns the command's name, used only for logging purposes.
   * @return The name of the implementation class.
   */
  default String getName() {
    return this.getClass().getSimpleName();
  }
}
