/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.callqueue.server;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.Time;

import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionManager {
  final private AtomicInteger count = new AtomicInteger();
  final private AtomicLong droppedConnections = new AtomicLong();
  final private Set<OzoneServer.Connection> connections;
  /* Map to maintain the statistics per User */
  final private Map<String, Integer> userToConnectionsMap;
  final private Object userToConnectionsMapLock = new Object();

  final private Timer idleScanTimer;
  final private int idleScanThreshold;
  final private int idleScanInterval;
  final private int maxIdleTime;
  final private int maxIdleToClose;
  final private int maxConnections;

  ConnectionManager() {
    this.idleScanTimer = new Timer(
        "IPC Server idle connection scanner for port " + getPort(), true);
    this.idleScanThreshold = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
    this.idleScanInterval = conf.getInt(
        CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
    this.maxIdleTime = 2 * conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
    this.maxIdleToClose = conf.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
        CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
    this.maxConnections = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_DEFAULT);
    // create a set with concurrency -and- a thread-safe iterator, add 2
    // for listener and idle closer threads
    this.connections = Collections.newSetFromMap(
        new ConcurrentHashMap<OzoneServer.Connection,Boolean>(
            maxQueueSize, 0.75f, readThreads+2));
    this.userToConnectionsMap = new ConcurrentHashMap<>();
  }

  private boolean add(OzoneServer.Connection connection) {
    boolean added = connections.add(connection);
    if (added) {
      count.getAndIncrement();
    }
    return added;
  }

  private boolean remove(OzoneServer.Connection connection) {
    boolean removed = connections.remove(connection);
    if (removed) {
      count.getAndDecrement();
    }
    return removed;
  }

  void incrUserConnections(String user) {
    synchronized (userToConnectionsMapLock) {
      Integer count = userToConnectionsMap.get(user);
      if (count == null) {
        count = 1;
      } else {
        count = count + 1;
      }
      userToConnectionsMap.put(user, count);
    }
  }

  void decrUserConnections(String user) {
    synchronized (userToConnectionsMapLock) {
      Integer count = userToConnectionsMap.get(user);
      if (count == null) {
        return;
      } else {
        count = count - 1;
      }
      if (count == 0) {
        userToConnectionsMap.remove(user);
      } else {
        userToConnectionsMap.put(user, count);
      }
    }
  }

  Map<String, Integer> getUserToConnectionsMap() {
    return userToConnectionsMap;
  }


  long getDroppedConnections() {
    return droppedConnections.get();
  }

  int size() {
    return count.get();
  }

  boolean isFull() {
    // The check is disabled when maxConnections <= 0.
    return ((maxConnections > 0) && (size() >= maxConnections));
  }

  OzoneServer.Connection[] toArray() {
    return connections.toArray(new OzoneServer.Connection[0]);
  }

  OzoneServer.Connection register(SocketChannel channel, int ingressPort,
                                  boolean isOnAuxiliaryPort) {
    if (isFull()) {
      return null;
    }
    OzoneServer.Connection connection = new OzoneServer.Connection(channel, Time.now(),
        ingressPort, isOnAuxiliaryPort);
    add(connection);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Server connection from " + connection +
          "; # active connections: " + size() +
          "; # queued calls: " + callQueue.size());
    }
    return connection;
  }

  boolean close(OzoneServer.Connection connection) {
    boolean exists = remove(connection);
    if (exists) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(Thread.currentThread().getName() +
            ": disconnecting client " + connection +
            ". Number of active connections: "+ size());
      }
      // only close if actually removed to avoid double-closing due
      // to possible races
      connection.close();
      // Remove authorized users only
      if (connection.user != null && connection.connectionContextRead) {
        decrUserConnections(connection.user.getShortUserName());
      }
    }
    return exists;
  }

  // synch'ed to avoid explicit invocation upon OOM from colliding with
  // timer task firing
  synchronized void closeIdle(boolean scanAll) {
    long minLastContact = Time.now() - maxIdleTime;
    // concurrent iterator might miss new connections added
    // during the iteration, but that's ok because they won't
    // be idle yet anyway and will be caught on next scan
    int closed = 0;
    for (OzoneServer.Connection connection : connections) {
      // stop if connections dropped below threshold unless scanning all
      if (!scanAll && size() < idleScanThreshold) {
        break;
      }
      // stop if not scanning all and max connections are closed
      if (connection.isIdle() &&
          connection.getLastContact() < minLastContact &&
          close(connection) &&
          !scanAll && (++closed == maxIdleToClose)) {
        break;
      }
    }
  }

  void closeAll() {
    // use a copy of the connections to be absolutely sure the concurrent
    // iterator doesn't miss a connection
    for (OzoneServer.Connection connection : toArray()) {
      close(connection);
    }
  }

  void startIdleScan() {
    scheduleIdleScanTask();
  }

  void stopIdleScan() {
    idleScanTimer.cancel();
  }

  private void scheduleIdleScanTask() {
    if (!running) {
      return;
    }
    TimerTask idleScanTask = new TimerTask(){
      @Override
      public void run() {
        if (!running) {
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName()+": task running");
        }
        try {
          closeIdle(false);
        } finally {
          // explicitly reschedule so next execution occurs relative
          // to the end of this scan, not the beginning
          scheduleIdleScanTask();
        }
      }
    };
    idleScanTimer.schedule(idleScanTask, idleScanInterval);
  }
}
