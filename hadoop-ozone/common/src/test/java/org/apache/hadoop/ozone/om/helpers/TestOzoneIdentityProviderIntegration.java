/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ipc.CallQueueManager;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Integration test class for {@link OzoneIdentityProvider}.
 */
public class TestOzoneIdentityProviderIntegration {

  private static CallQueueManager callQueueManager;
  private static FairCallQueue fairCallQueue;
  private static DecayRpcScheduler decayRpcScheduler;
  private static OzoneIdentityProvider identityProvider;
  private static final UserGroupInformation UGI =
      UserGroupInformation.createRemoteUser("s3g");
  private static final String ACCESS_ID = "testuser";
  private static final CallerContext CALLER_CONTEXT =
      new CallerContext.Builder(ACCESS_ID).build();

  @BeforeAll
  public static void setUp() {
    identityProvider = new OzoneIdentityProvider();
    callQueueManager = Mockito.mock(CallQueueManager.class);
    fairCallQueue = Mockito.mock(FairCallQueue.class);
    decayRpcScheduler = Mockito.mock(DecayRpcScheduler.class);
  }

  @Test
  public void testUserIdentity() {
    Server.Call call = spy(new Server.Call(1, 1, null, null,
        RPC.RpcKind.RPC_BUILTIN, new byte[] {1, 2, 3}));
    Server.getCurCall().set(call);

    when(call.getUserGroupInformation())
        .thenReturn(UGI);
    when(call.getCallerContext()).thenReturn(CALLER_CONTEXT);
  }
}
