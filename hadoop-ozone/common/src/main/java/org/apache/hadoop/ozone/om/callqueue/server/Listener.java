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

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Listens on the socket. Creates jobs for the handler threads*/
public class Listener extends Thread {

  private ServerSocketChannel acceptChannel = null; //the accept channel
  private Selector selector = null; //the selector that we use for the server
  private Listener.Reader[] readers = null;
  private int currentReader = 0;
  private InetSocketAddress address; //the address we bind at
  private int listenPort; //the port we bind at
  private int backlogLength = conf.getInt(
      CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
      CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
  private boolean reuseAddr = conf.getBoolean(
      CommonConfigurationKeysPublic.IPC_SERVER_REUSEADDR_KEY,
      CommonConfigurationKeysPublic.IPC_SERVER_REUSEADDR_DEFAULT);
  private boolean isOnAuxiliaryPort;

  Listener(int port) throws IOException {
    address = new InetSocketAddress(bindAddress, port);
    // Create a new server socket and set to non blocking mode
    acceptChannel = ServerSocketChannel.open();
    acceptChannel.configureBlocking(false);
    acceptChannel.setOption(StandardSocketOptions.SO_REUSEADDR, reuseAddr);

    // Bind the server socket to the local host and port
    bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
    //Could be an ephemeral port
    this.listenPort = acceptChannel.socket().getLocalPort();
    Thread.currentThread().setName("Listener at " +
        bindAddress + "/" + this.listenPort);
    // create a selector;
    selector= Selector.open();
    readers = new Listener.Reader[readThreads];
    for (int i = 0; i < readThreads; i++) {
      Listener.Reader reader = new Listener.Reader(
          "Socket Reader #" + (i + 1) + " for port " + port);
      readers[i] = reader;
      reader.start();
    }

    // Register accepts on the server socket with the selector.
    acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
    this.setName("IPC Server listener on " + port);
    this.setDaemon(true);
    this.isOnAuxiliaryPort = false;
  }

  void setIsAuxiliary() {
    this.isOnAuxiliaryPort = true;
  }

  private class Reader extends Thread {
    final private BlockingQueue<Connection> pendingConnections;
    private final Selector readSelector;

    Reader(String name) throws IOException {
      super(name);

      this.pendingConnections =
          new LinkedBlockingQueue<>(readerPendingConnectionQueue);
      this.readSelector = Selector.open();
    }

    @Override
    public void run() {
      LOG.info("Starting " + Thread.currentThread().getName());
      try {
        doRunLoop();
      } finally {
        try {
          readSelector.close();
        } catch (IOException ioe) {
          LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
        }
      }
    }

    private synchronized void doRunLoop() {
      while (running) {
        SelectionKey key = null;
        try {
          // consume as many connections as currently queued to avoid
          // unbridled acceptance of connections that starves the select
          int size = pendingConnections.size();
          for (int i=size; i>0; i--) {
            Connection conn = pendingConnections.take();
            conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
          }
          readSelector.select();

          Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isReadable()) {
                doRead(key);
              }
            } catch (CancelledKeyException cke) {
              // something else closed the connection, ex. responder or
              // the listener doing an idle scan.  ignore it and let them
              // clean up.
              LOG.info(Thread.currentThread().getName() +
                  ": connection aborted from " + key.attachment());
            }
            key = null;
          }
        } catch (InterruptedException e) {
          if (running) {                      // unexpected -- log it
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
          }
        } catch (IOException ex) {
          LOG.error("Error in Reader", ex);
        } catch (Throwable re) {
          LOG.error("Bug in read selector!", re);
          ExitUtil.terminate(1, "Bug in read selector!");
        }
      }
    }

    /**
     * Updating the readSelector while it's being used is not thread-safe,
     * so the connection must be queued.  The reader will drain the queue
     * and update its readSelector before performing the next select
     */
    public void addConnection(Connection conn) throws InterruptedException {
      pendingConnections.put(conn);
      readSelector.wakeup();
    }

    void shutdown() {
      assert !running;
      readSelector.wakeup();
      try {
        super.interrupt();
        super.join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void run() {
    LOG.info(Thread.currentThread().getName() + ": starting");
    SERVER.set(OzoneServer.this);
    connectionManager.startIdleScan();
    while (running) {
      SelectionKey key = null;
      try {
        getSelector().select();
        Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
        while (iter.hasNext()) {
          key = iter.next();
          iter.remove();
          try {
            if (key.isValid()) {
              if (key.isAcceptable())
                doAccept(key);
            }
          } catch (IOException e) {
          }
          key = null;
        }
      } catch (OutOfMemoryError e) {
        // we can run out of memory if we have too many threads
        // log the event and sleep for a minute and give
        // some thread(s) a chance to finish
        LOG.warn("Out of Memory in server select", e);
        closeCurrentConnection(key, e);
        connectionManager.closeIdle(true);
        try { Thread.sleep(60000); } catch (Exception ie) {}
      } catch (Exception e) {
        closeCurrentConnection(key, e);
      }
    }
    LOG.info("Stopping " + Thread.currentThread().getName());

    synchronized (this) {
      try {
        acceptChannel.close();
        selector.close();
      } catch (IOException e) { }

      selector= null;
      acceptChannel= null;

      // close all connections
      connectionManager.stopIdleScan();
      connectionManager.closeAll();
    }
  }

  private void closeCurrentConnection(SelectionKey key, Throwable e) {
    if (key != null) {
      Connection c = (Connection)key.attachment();
      if (c != null) {
        closeConnection(c);
        c = null;
      }
    }
  }

  InetSocketAddress getAddress() {
    return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
  }

  void doAccept(SelectionKey key) throws InterruptedException, IOException,  OutOfMemoryError {
    ServerSocketChannel server = (ServerSocketChannel) key.channel();
    SocketChannel channel;
    while ((channel = server.accept()) != null) {

      channel.configureBlocking(false);
      channel.socket().setTcpNoDelay(tcpNoDelay);
      channel.socket().setKeepAlive(true);

      Listener.Reader reader = getReader();
      Connection c = connectionManager.register(channel,
          this.listenPort, this.isOnAuxiliaryPort);
      // If the connectionManager can't take it, close the connection.
      if (c == null) {
        if (channel.isOpen()) {
          IOUtils.cleanupWithLogger(LOG, channel);
        }
        connectionManager.droppedConnections.getAndIncrement();
        continue;
      }
      key.attach(c);  // so closeCurrentConnection can get the object
      reader.addConnection(c);
    }
  }

  void doRead(SelectionKey key) throws InterruptedException {
    int count;
    Connection c = (Connection)key.attachment();
    if (c == null) {
      return;
    }
    c.setLastContact(Time.now());

    try {
      count = c.readAndProcess();
    } catch (InterruptedException ieo) {
      LOG.info(Thread.currentThread().getName() + ": readAndProcess caught InterruptedException", ieo);
      throw ieo;
    } catch (Exception e) {
      // Any exceptions that reach here are fatal unexpected internal errors
      // that could not be sent to the client.
      LOG.info(Thread.currentThread().getName() +
          ": readAndProcess from client " + c +
          " threw exception [" + e + "]", e);
      count = -1; //so that the (count < 0) block is executed
    }
    // setupResponse will signal the connection should be closed when a
    // fatal response is sent.
    if (count < 0 || c.shouldClose()) {
      closeConnection(c);
      c = null;
    }
    else {
      c.setLastContact(Time.now());
    }
  }

  synchronized void doStop() {
    if (selector != null) {
      selector.wakeup();
      Thread.yield();
    }
    if (acceptChannel != null) {
      try {
        acceptChannel.socket().close();
      } catch (IOException e) {
        LOG.info(Thread.currentThread().getName() + ":Exception in closing listener socket. " + e);
      }
    }
    for (Listener.Reader r : readers) {
      r.shutdown();
    }
  }

  synchronized Selector getSelector() { return selector; }
  // The method that will return the next reader to work with
  // Simplistic implementation of round robin for now
  Listener.Reader getReader() {
    currentReader = (currentReader + 1) % readers.length;
    return readers[currentReader];
  }
}
