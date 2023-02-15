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

import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

// Sends responses of RPC back to clients.
public class Responder extends Thread {
  private final Selector writeSelector;
  private int pending;         // connections waiting to register

  Responder() throws IOException {
    this.setName("IPC Server Responder");
    this.setDaemon(true);
    writeSelector = Selector.open(); // create a selector
    pending = 0;
  }

  @Override
  public void run() {
    LOG.info(Thread.currentThread().getName() + ": starting");
    SERVER.set(OzoneServer.this);
    try {
      doRunLoop();
    } finally {
      LOG.info("Stopping " + Thread.currentThread().getName());
      try {
        writeSelector.close();
      } catch (IOException ioe) {
        LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
      }
    }
  }

  private void doRunLoop() {
    long lastPurgeTimeNanos = 0;   // last check for old calls.

    while (running) {
      try {
        waitPending();     // If a channel is being registered, wait.
        writeSelector.select(
            TimeUnit.NANOSECONDS.toMillis(purgeIntervalNanos));
        Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          iter.remove();
          try {
            if (key.isWritable()) {
              doAsyncWrite(key);
            }
          } catch (CancelledKeyException cke) {
            // something else closed the connection, ex. reader or the
            // listener doing an idle scan.  ignore it and let them clean
            // up
            RpcCall call = (RpcCall)key.attachment();
            if (call != null) {
              LOG.info(Thread.currentThread().getName() +
                  ": connection aborted from " + call.connection);
            }
          } catch (IOException e) {
            LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + e);
          }
        }
        long nowNanos = Time.monotonicNowNanos();
        if (nowNanos < lastPurgeTimeNanos + purgeIntervalNanos) {
          continue;
        }
        lastPurgeTimeNanos = nowNanos;
        //
        // If there were some calls that have not been sent out for a
        // long time, discard them.
        //
        if(LOG.isDebugEnabled()) {
          LOG.debug("Checking for old call responses.");
        }
        ArrayList<RpcCall> calls;

        // get the list of channels from list of keys.
        synchronized (writeSelector.keys()) {
          calls = new ArrayList<RpcCall>(writeSelector.keys().size());
          iter = writeSelector.keys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            RpcCall call = (RpcCall)key.attachment();
            if (call != null && key.channel() == call.connection.channel) {
              calls.add(call);
            }
          }
        }

        for (RpcCall call : calls) {
          doPurge(call, nowNanos);
        }
      } catch (OutOfMemoryError e) {
        //
        // we can run out of memory if we have too many threads
        // log the event and sleep for a minute and give
        // some thread(s) a chance to finish
        //
        LOG.warn("Out of Memory in server select", e);
        try { Thread.sleep(60000); } catch (Exception ie) {}
      } catch (Exception e) {
        LOG.warn("Exception in Responder", e);
      }
    }
  }

  private void doAsyncWrite(SelectionKey key) throws IOException {
    RpcCall call = (RpcCall)key.attachment();
    if (call == null) {
      return;
    }
    if (key.channel() != call.connection.channel) {
      throw new IOException("doAsyncWrite: bad channel");
    }

    synchronized(call.connection.responseQueue) {
      if (processResponse(call.connection.responseQueue, false)) {
        try {
          key.interestOps(0);
        } catch (CancelledKeyException e) {
          /* The Listener/reader might have closed the socket.
           * We don't explicitly cancel the key, so not sure if this will
           * ever fire.
           * This warning could be removed.
           */
          LOG.warn("Exception while changing ops : " + e);
        }
      }
    }
  }

  //
  // Remove calls that have been pending in the responseQueue
  // for a long time.
  //
  private void doPurge(RpcCall call, long now) {
    LinkedList<RpcCall> responseQueue = call.connection.responseQueue;
    synchronized (responseQueue) {
      Iterator<RpcCall> iter = responseQueue.listIterator(0);
      while (iter.hasNext()) {
        call = iter.next();
        if (now > call.responseTimestampNanos + purgeIntervalNanos) {
          closeConnection(call.connection);
          break;
        }
      }
    }
  }

  // Processes one response. Returns true if there are no more pending
  // data for this channel.
  //
  private boolean processResponse(LinkedList<RpcCall> responseQueue,
                                  boolean inHandler) throws IOException {
    boolean error = true;
    boolean done = false;       // there is more data for this channel.
    int numElements = 0;
    RpcCall call = null;
    try {
      synchronized (responseQueue) {
        //
        // If there are no items for this channel, then we are done
        //
        numElements = responseQueue.size();
        if (numElements == 0) {
          error = false;
          return true;              // no more data for this channel.
        }
        //
        // Extract the first call
        //
        call = responseQueue.removeFirst();
        SocketChannel channel = call.connection.channel;
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
        }
        //
        // Send as much data as we can in the non-blocking fashion
        //
        int numBytes = channelWrite(channel, call.rpcResponse);
        if (numBytes < 0) {
          return true;
        }
        if (!call.rpcResponse.hasRemaining()) {
          //Clear out the response buffer so it can be collected
          call.rpcResponse = null;
          call.connection.decRpcCount();
          if (numElements == 1) {    // last call fully processes.
            done = true;             // no more data for this channel.
          } else {
            done = false;            // more calls pending to be sent.
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                + " Wrote " + numBytes + " bytes.");
          }
        } else {
          //
          // If we were unable to write the entire response out, then
          // insert in Selector queue.
          //
          call.connection.responseQueue.addFirst(call);

          if (inHandler) {
            // set the serve time when the response has to be sent later
            call.responseTimestampNanos = Time.monotonicNowNanos();

            incPending();
            try {
              // Wakeup the thread blocked on select, only then can the call
              // to channel.register() complete.
              writeSelector.wakeup();
              channel.register(writeSelector, SelectionKey.OP_WRITE, call);
            } catch (ClosedChannelException e) {
              //Its ok. channel might be closed else where.
              done = true;
            } finally {
              decPending();
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                + " Wrote partial " + numBytes + " bytes.");
          }
        }
        error = false;              // everything went off well
      }
    } finally {
      if (error && call != null) {
        LOG.warn(Thread.currentThread().getName()+", call " + call + ": output error");
        done = true;               // error. no more data for this channel.
        closeConnection(call.connection);
      }
    }
    return done;
  }

  //
  // Enqueue a response from the application.
  //
  void doRespond(RpcCall call) throws IOException {
    synchronized (call.connection.responseQueue) {
      // must only wrap before adding to the responseQueue to prevent
      // postponed responses from being encrypted and sent out of order.
      if (call.connection.useWrap) {
        wrapWithSasl(call);
      }
      call.connection.responseQueue.addLast(call);
      if (call.connection.responseQueue.size() == 1) {
        processResponse(call.connection.responseQueue, true);
      }
    }
  }

  private synchronized void incPending() {   // call waiting to be enqueued.
    pending++;
  }

  private synchronized void decPending() { // call done enqueueing.
    pending--;
    notify();
  }

  private synchronized void waitPending() throws InterruptedException {
    while (pending > 0) {
      wait();
    }
  }
}
