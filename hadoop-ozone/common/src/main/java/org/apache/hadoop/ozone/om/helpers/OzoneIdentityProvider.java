/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import io.netty.util.internal.StringUtil;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.IdentityProvider;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Ozone implementation of IdentityProvider used by
 * Hadoop DecayRpcScheduler.
 */
public class OzoneIdentityProvider implements IdentityProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneIdentityProvider.class);

  public OzoneIdentityProvider() {
  }

  /**
   * If schedulable isn't instance of {@link Server.Call},
   * then trying to access getCallerContext() method, will
   * result in an exception.
   *
   * @param schedulable Schedulable object
   * @return string with the identity of the user
   */
  @Override
  public String makeIdentity(Schedulable schedulable) {
    UserGroupInformation ugi = schedulable.getUserGroupInformation();
    try {
      CallerContext callerContext = schedulable.getCallerContext();
      if (Objects.nonNull(callerContext) &&
          !StringUtil.isNullOrEmpty(callerContext.getContext())) {
        LOG.info("xbis: CC identity: " + callerContext.getContext());
        return callerContext.getContext();
      }
    } catch (UnsupportedOperationException ex) {
      LOG.error("Trying to access CallerContext from a Schedulable " +
          "implementation that's not instance of Server.Call");
    }
    LOG.info("xbis: UGI identity: " + ugi.getShortUserName());
    return ugi.getShortUserName() == null ? null : ugi.getShortUserName();
  }
}
