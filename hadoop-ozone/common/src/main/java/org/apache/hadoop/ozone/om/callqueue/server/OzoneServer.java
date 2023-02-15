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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.DecayRpcScheduler;
import org.apache.hadoop.ipc.DefaultRpcScheduler;
import org.apache.hadoop.ipc.FairCallQueue;
import org.apache.hadoop.ipc.IpcException;
import org.apache.hadoop.ipc.ProcessingDetails;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.RpcClientUtil;
import org.apache.hadoop.ipc.RpcConstants;
import org.apache.hadoop.ipc.RpcScheduler;
import org.apache.hadoop.ipc.RpcServerException;
import org.apache.hadoop.ipc.RpcWritable;
import org.apache.hadoop.ipc.Schedulable;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.callqueue.OzoneCallQueueManager;
import org.apache.hadoop.ozone.om.callqueue.OzoneRPC;
import org.apache.hadoop.ozone.om.callqueue.OzoneResponseBuffer;
import org.apache.hadoop.ozone.om.callqueue.OzoneRpcWritable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.apache.hadoop.tracing.Span;
import org.apache.hadoop.tracing.SpanContext;
import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.hadoop.tracing.Tracer;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hadoop.ipc.RpcConstants.AUTHORIZATION_FAILED_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.HEADER_LEN_AFTER_HRPC_PART;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

public abstract class OzoneServer {
  private final boolean authorize;
  private List<SaslRpcServer.AuthMethod> enabledAuthMethods;
  private RpcHeaderProtos.RpcSaslProto negotiateResponse;
  private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
  private Tracer tracer;
  private AlignmentContext alignmentContext;
  /**
   * Logical name of the server used in metrics and monitor.
   */
  private final String serverName;

  /**
   * Add exception classes for which server won't log stack traces.
   *
   * @param exceptionClass exception classes
   */
  public void addTerseExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addTerseLoggingExceptions(exceptionClass);
  }

  /**
   * Add exception classes which server won't log at all.
   *
   * @param exceptionClass exception classes
   */
  public void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addSuppressedLoggingExceptions(exceptionClass);
  }

  /**
   * Set alignment context to pass state info thru RPC.
   *
   * @param alignmentContext alignment state context
   */
  public void setAlignmentContext(AlignmentContext alignmentContext) {
    this.alignmentContext = alignmentContext;
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {

    private final Set<String> terseExceptions =
        ConcurrentHashMap.newKeySet();
    private final Set<String> suppressedExceptions =
        ConcurrentHashMap.newKeySet();

    /**
     * Add exception classes for which server won't log stack traces.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes
     */
    void addTerseLoggingExceptions(Class<?>... exceptionClass) {
      terseExceptions.addAll(Arrays
          .stream(exceptionClass)
          .map(Class::toString)
          .collect(Collectors.toSet()));
    }

    /**
     * Add exception classes which server won't log at all.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes
     */
    void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
      suppressedExceptions.addAll(Arrays
          .stream(exceptionClass)
          .map(Class::toString)
          .collect(Collectors.toSet()));
    }

    boolean isTerseLog(Class<?> t) {
      return terseExceptions.contains(t.toString());
    }

    boolean isSuppressedLog(Class<?> t) {
      return suppressedExceptions.contains(t.toString());
    }

  }


  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes(StandardCharsets.UTF_8));

  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final String RECEIVED_HTTP_REQ_RESPONSE =
      "HTTP/1.1 404 Not Found\r\n" +
          "Content-type: text/plain\r\n\r\n" +
          "It looks like you are making an HTTP request to a Hadoop IPC port. " +
          "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static int INITIAL_RESP_BUF_SIZE = 10240;

  static class RpcKindMapValue {
    final Class<? extends Writable> rpcRequestWrapperClass;
    final OzoneRPC.RpcInvoker rpcInvoker;

    RpcKindMapValue (Class<? extends Writable> rpcRequestWrapperClass,
                     OzoneRPC.RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }
  }
  static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>(4);



  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   *
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */

  public static void registerProtocolEngine(RPC.RpcKind rpcKind,
                                            Class<? extends Writable> rpcRequestWrapperClass,
                                            OzoneRPC.RpcInvoker rpcInvoker) {
    RpcKindMapValue old =
        rpcKindMap.put(rpcKind, new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcKind=" + rpcKind +
          ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
          ", rpcInvoker=" + rpcInvoker);
    }
  }

  public Class<? extends Writable> getRpcRequestWrapper(
      RpcHeaderProtos.RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
      return rpcRequestClass;
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass;
  }

  protected OzoneRPC.RpcInvoker getServerRpcInvoker(RPC.RpcKind rpcKind) {
    return getRpcInvoker(rpcKind);
  }

  public static OzoneRPC.RpcInvoker getRpcInvoker(RPC.RpcKind rpcKind) {
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker;
  }


  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneServer.class);
  public static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger."+OzoneServer.class.getName());
  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";

  private static final ThreadLocal<OzoneServer> SERVER = new ThreadLocal<>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE =
      new ConcurrentHashMap<String, Class<?>>();

  static Class<?> getProtocolClass(String protocolName, Configuration conf)
      throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static OzoneServer get() {
    return SERVER.get();
  }

  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<>();

  /** Get the current call */
  @VisibleForTesting
  public static ThreadLocal<Call> getCurCall() {
    return CurCall;
  }

  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   *
   * @return int sequential ID number of currently active RPC call
   */
  public static int getCallId() {
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }

  /**
   * @return The current active RPC call's retry count. -1 indicates the retry
   *         cache is not supported in the client side.
   */
  public static int getCallRetryCount() {
    Call call = CurCall.get();
    return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
  }

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    return (call != null ) ? call.getHostInetAddress() : null;
  }

  /**
   * Returns the SASL qop for the current call, if the current call is
   * set, and the SASL negotiation is done. Otherwise return null
   * Note this only returns established QOP for auxiliary port, and
   * returns null for primary (non-auxiliary) port.
   *
   * Also note that CurCall is thread local object. So in fact, different
   * handler threads will process different CurCall object.
   *
   * Also, only return for RPC calls, not supported for other protocols.
   * @return the QOP of the current connection.
   */
  public static String getAuxiliaryPortEstablishedQOP() {
    Call call = CurCall.get();
    if (!(call instanceof RpcCall)) {
      return null;
    }
    RpcCall rpcCall = (RpcCall)call;
    if (rpcCall.connection.isOnAuxiliaryPort()) {
      return rpcCall.connection.getEstablishedQOP();
    } else {
      // Not sending back QOP for primary port
      return null;
    }
  }

  /**
   * Returns the clientId from the current RPC request
   */
  public static byte[] getClientId() {
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }

  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static UserGroupInformation getRemoteUser() {
    Call call = CurCall.get();
    return (call != null) ? call.getRemoteUser() : null;
  }

  public static String getProtocol() {
    Call call = CurCall.get();
    return (call != null) ? call.getProtocol() : null;
  }

  /** Return true if the invocation was through an RPC.
   */
  public static boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  /**
   * Return the priority level assigned by call queue to an RPC
   * Returns 0 in case no priority is assigned.
   */
  public static int getPriorityLevel() {
    Call call = CurCall.get();
    return call != null? call.getPriorityLevel() : 0;
  }

  private String bindAddress;
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int readThreads;                        // number of read threads
  private int readerPendingConnectionQueue;         // number of connections to queue per read thread
  private Class<? extends Writable> rpcRequestClass;   // class used for deserializing the rpc request
  final protected RpcMetrics rpcMetrics;
  final protected RpcDetailedMetrics rpcDetailedMetrics;

  private Configuration conf;
  private String portRangeConfig = null;
  private SecretManager<TokenIdentifier> secretManager;
  private SaslPropertiesResolver saslPropsResolver;
  private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager();

  private int maxQueueSize;
  private final int maxRespSize;
  private final ThreadLocal<OzoneResponseBuffer> responseBuffer =
      ThreadLocal.withInitial(() -> new OzoneResponseBuffer(INITIAL_RESP_BUF_SIZE));
  private int socketSendBufferSize;
  private final int maxDataLength;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private boolean running = true;         // true while server runs
  private OzoneCallQueueManager<Call> callQueue;

  private long purgeIntervalNanos;

  // maintains the set of client connections and handles idle timeouts
  private ConnectionManager connectionManager;
  private Listener listener = null;
  // Auxiliary listeners maintained as in a map, to allow
  // arbitrary number of of auxiliary listeners. A map from
  // the port to the listener binding to it.
  private Map<Integer, Listener> auxiliaryListenerMap;
  private Responder responder = null;
  private Handler[] handlers = null;

  private boolean logSlowRPC = false;

  /**
   * Checks if LogSlowRPC is set true.
   * @return true, if LogSlowRPC is set true, false, otherwise.
   */
  protected boolean isLogSlowRPC() {
    return logSlowRPC;
  }

  /**
   * Sets slow RPC flag.
   * @param logSlowRPCFlag
   */
  @VisibleForTesting
  protected void setLogSlowRPC(boolean logSlowRPCFlag) {
    this.logSlowRPC = logSlowRPCFlag;
  }

  private void setPurgeIntervalNanos(int purgeInterval) {
    int tmpPurgeInterval = CommonConfigurationKeysPublic.
        IPC_SERVER_PURGE_INTERVAL_MINUTES_DEFAULT;
    if (purgeInterval > 0) {
      tmpPurgeInterval = purgeInterval;
    }
    this.purgeIntervalNanos = TimeUnit.NANOSECONDS.convert(
        tmpPurgeInterval, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  public long getPurgeIntervalNanos() {
    return this.purgeIntervalNanos;
  }

  /**
   * Logs a Slow RPC Request.
   *
   * @param methodName - RPC Request method name
   * @param details - Processing Detail.
   *
   * if this request took too much time relative to other requests
   * we consider that as a slow RPC. 3 is a magic number that comes
   * from 3 sigma deviation. A very simple explanation can be found
   * by searching for 68-95-99.7 rule. We flag an RPC as slow RPC
   * if and only if it falls above 99.7% of requests. We start this logic
   * only once we have enough sample size.
   */
  void logSlowRpcCalls(String methodName, Call call,
                       ProcessingDetails details) {
    final int deviation = 3;

    // 1024 for minSampleSize just a guess -- not a number computed based on
    // sample size analysis. It is chosen with the hope that this
    // number is high enough to avoid spurious logging, yet useful
    // in practice.
    final int minSampleSize = 1024;
    final double threeSigma = rpcMetrics.getProcessingMean() +
        (rpcMetrics.getProcessingStdDev() * deviation);

    long processingTime =
        details.get(ProcessingDetails.Timing.PROCESSING, rpcMetrics.getMetricsTimeUnit());
    if ((rpcMetrics.getProcessingSampleCount() > minSampleSize) &&
        (processingTime > threeSigma)) {
      LOG.warn(
          "Slow RPC : {} took {} {} to process from client {},"
              + " the processing detail is {}",
          methodName, processingTime, rpcMetrics.getMetricsTimeUnit(), call,
          details.toString());
      rpcMetrics.incrSlowRpc();
    }
  }

  void updateMetrics(Call call, long startTime, boolean connDropped) {
    // delta = handler + processing + response
    long deltaNanos = Time.monotonicNowNanos() - startTime;
    long timestampNanos = call.timestampNanos;

    OzoneProcessingDetails details = call.getProcessingDetails();
    // queue time is the delta between when the call first arrived and when it
    // began being serviced, minus the time it took to be put into the queue
    details.set(OzoneProcessingDetails.Timing.QUEUE,
        startTime - timestampNanos - details.get(OzoneProcessingDetails.Timing.ENQUEUE));
    deltaNanos -= details.get(OzoneProcessingDetails.Timing.PROCESSING);
    deltaNanos -= details.get(OzoneProcessingDetails.Timing.RESPONSE);
    details.set(OzoneProcessingDetails.Timing.HANDLER, deltaNanos);

    long queueTime = details.get(OzoneProcessingDetails.Timing.QUEUE, rpcMetrics.getMetricsTimeUnit());
    rpcMetrics.addRpcQueueTime(queueTime);

    if (call.isResponseDeferred() || connDropped) {
      // call was skipped; don't include it in processing metrics
      return;
    }

    long processingTime =
        details.get(OzoneProcessingDetails.Timing.PROCESSING, rpcMetrics.getMetricsTimeUnit());
    long waitTime =
        details.get(OzoneProcessingDetails.Timing.LOCKWAIT, rpcMetrics.getMetricsTimeUnit());
    rpcMetrics.addRpcLockWaitTime(waitTime);
    rpcMetrics.addRpcProcessingTime(processingTime);
    // don't include lock wait for detailed metrics.
    processingTime -= waitTime;
    String name = call.getDetailedMetricsName();
    rpcDetailedMetrics.addProcessingTime(name, processingTime);
//    callQueue.addResponseTime(name, call, details);
//    if (isLogSlowRPC()) {
//      logSlowRpcCalls(name, call, details);
//    }
  }

  void updateDeferredMetrics(String name, long processingTime) {
    rpcMetrics.addDeferredRpcProcessingTime(processingTime);
    rpcDetailedMetrics.addDeferredProcessingTime(name, processingTime);
  }

  /**
   * A convenience method to bind to a given address and report
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog, Configuration conf, String rangeConf) throws IOException {
    try {
      Configuration.IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (Integer port : range) {
          if (socket.isBound()) break;
          try {
            InetSocketAddress temp = new InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new BindException("Could not find a free port in "+range);
        }
      }
    } catch (SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }

  @VisibleForTesting
  int getPriorityLevel(Schedulable e) {
    return callQueue.getPriorityLevel(e);
  }

  @VisibleForTesting
  int getPriorityLevel(UserGroupInformation ugi) {
    return callQueue.getPriorityLevel(ugi);
  }

  @VisibleForTesting
  void setPriorityLevel(UserGroupInformation ugi, int priority) {
    callQueue.setPriorityLevel(ugi, priority);
  }

  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public RpcMetrics getRpcMetrics() {
    return rpcMetrics;
  }

  @VisibleForTesting
  public RpcDetailedMetrics getRpcDetailedMetrics() {
    return rpcDetailedMetrics;
  }

  @VisibleForTesting
  Iterable<? extends Thread> getHandlers() {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  Connection[] getConnections() {
    return connectionManager.toArray();
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(Configuration conf, PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server
   * using the specified Configuration.
   */
  @InterfaceAudience.Private
  public void refreshServiceAclWithLoadedConfiguration(Configuration conf,
                                                       PolicyProvider provider) {
    serviceAuthorizationManager.refreshWithLoadedConfiguration(conf, provider);
  }
  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public ServiceAuthorizationManager getServiceAuthorizationManager() {
    return serviceAuthorizationManager;
  }

  private String getQueueClassPrefix() {
    return CommonConfigurationKeys.IPC_NAMESPACE + "." + port;
  }

  static Class<? extends BlockingQueue<Call>> getQueueClass(
      String prefix, Configuration conf) {
    String name = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
    Class<?> queueClass = conf.getClass(name, LinkedBlockingQueue.class);
    return OzoneCallQueueManager.convertQueueClass(queueClass, Call.class);
  }

  static Class<? extends RpcScheduler> getSchedulerClass(
      String prefix, Configuration conf) {
    String schedulerKeyname = prefix + "." + CommonConfigurationKeys
        .IPC_SCHEDULER_IMPL_KEY;
    Class<?> schedulerClass = conf.getClass(schedulerKeyname, null);
    // Patch the configuration for legacy fcq configuration that does not have
    // a separate scheduler setting
    if (schedulerClass == null) {
      String queueKeyName = prefix + "." + CommonConfigurationKeys
          .IPC_CALLQUEUE_IMPL_KEY;
      Class<?> queueClass = conf.getClass(queueKeyName, null);
      if (queueClass != null) {
        if (queueClass.getCanonicalName().equals(
            FairCallQueue.class.getCanonicalName())) {
          conf.setClass(schedulerKeyname, DecayRpcScheduler.class,
              RpcScheduler.class);
        }
      }
    }
    schedulerClass = conf.getClass(schedulerKeyname,
        DefaultRpcScheduler.class);

    return OzoneCallQueueManager.convertSchedulerClass(schedulerClass);
  }

  /*
   * Refresh the call queue
   */
  public synchronized void refreshCallQueue(Configuration conf) {
    // Create the next queue
    String prefix = getQueueClassPrefix();
    this.maxQueueSize = handlerCount * conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    callQueue.swapQueue(getSchedulerClass(prefix, conf),
        getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
    callQueue.setClientBackoffEnabled(getClientBackoffEnable(prefix, conf));
  }

  /**
   * Get from config if client backoff is enabled on that port.
   */
  static boolean getClientBackoffEnable(
      String prefix, Configuration conf) {
    String name = prefix + "." +
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
    return conf.getBoolean(name,
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
  }

  @InterfaceAudience.Private
  public enum AuthProtocol {
    NONE(0),
    SASL(-33);

    public final int callId;
    AuthProtocol(int callId) {
      this.callId = callId;
    }

    static AuthProtocol valueOf(int callId) {
      for (AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };

  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error.
   */
  @SuppressWarnings("serial")
  private static class FatalRpcServerException extends RpcServerException {
    private final RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode;
    public FatalRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode, IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public FatalRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode, String message) {
      this(errCode, new RpcServerException(message));
    }
    @Override
    public RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto getRpcStatusProto() {
      return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL;
    }
    @Override
    public RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto getRpcErrorCodeProto() {
      return errCode;
    }
    @Override
    public String toString() {
      return getCause().toString();
    }
  }

  public void queueCall(Call call) throws IOException, InterruptedException {
    // external non-rpc calls don't need server exception wrapper.
    try {
      internalQueueCall(call);
    } catch (RpcServerException rse) {
      throw (IOException)rse.getCause();
    }
  }

  private void internalQueueCall(Call call)
      throws IOException, InterruptedException {
    internalQueueCall(call, true);
  }

  private void internalQueueCall(Call call, boolean blocking)
      throws IOException, InterruptedException {
    try {
      // queue the call, may be blocked if blocking is true.
      if (blocking) {
        callQueue.put(call);
      } else {
        callQueue.add(call);
      }
      long deltaNanos = Time.monotonicNowNanos() - call.timestampNanos;
      call.getProcessingDetails().set(OzoneProcessingDetails.Timing.ENQUEUE, deltaNanos,
          TimeUnit.NANOSECONDS);
    } catch (OzoneCallQueueManager.CallQueueOverflowException cqe) {
      // If rpc scheduler indicates back off based on performance degradation
      // such as response time or rpc queue is full, we will ask the client
      // to back off by throwing RetriableException. Whether the client will
      // honor RetriableException and retry depends the client and its policy.
      // For example, IPC clients using FailoverOnNetworkExceptionRetry handle
      // RetriableException.
      rpcMetrics.incrClientBackoff();
      // unwrap retriable exception.
      throw cqe.getCause();
    }
  }

  @VisibleForTesting
  void logException(Logger logger, Throwable e, Call call) {
    if (exceptionsHandler.isSuppressedLog(e.getClass())) {
      return; // Log nothing.
    }

    final String logMsg = Thread.currentThread().getName() + ", call " + call;
    if (exceptionsHandler.isTerseLog(e.getClass())) {
      // Don't log the whole stack trace. Way too noisy!
      logger.info(logMsg + ": " + e);
    } else if (e instanceof RuntimeException || e instanceof Error) {
      // These exception types indicate something is probably wrong
      // on the server side, as opposed to just a normal exceptional
      // result.
      logger.warn(logMsg, e);
    } else {
      logger.info(logMsg, e);
    }
  }

  protected OzoneServer(String bindAddress, int port,
                   Class<? extends Writable> paramClass, int handlerCount,
                   Configuration conf)
      throws IOException
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }

  protected OzoneServer(String bindAddress, int port,
                   Class<? extends Writable> rpcRequestClass, int handlerCount,
                   int numReaders, int queueSizePerHandler, Configuration conf,
                   String serverName, SecretManager<? extends TokenIdentifier> secretManager)
      throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders,
        queueSizePerHandler, conf, serverName, secretManager, null);
  }

  /**
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</code> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   *
   * If rpcRequestClass is null then the rpcRequestClass must have been
   * registered via {@link #registerProtocolEngine(OzoneRPC.RpcKind,
   *  Class, OzoneRPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected OzoneServer(String bindAddress, int port,
                   Class<? extends Writable> rpcRequestClass, int handlerCount,
                   int numReaders, int queueSizePerHandler, Configuration conf,
                   String serverName, SecretManager<? extends TokenIdentifier> secretManager,
                   String portRangeConfig)
      throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;
    this.port = port;
    this.rpcRequestClass = rpcRequestClass;
    this.handlerCount = handlerCount;
    this.socketSendBufferSize = 0;
    this.serverName = serverName;
    this.auxiliaryListenerMap = null;
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
    if (queueSizePerHandler != -1) {
      this.maxQueueSize = handlerCount * queueSizePerHandler;
    } else {
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    }
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }
    this.readerPendingConnectionQueue = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);

    // Setup appropriate callqueue
    final String prefix = getQueueClassPrefix();
    this.callQueue = new OzoneCallQueueManager<>(getQueueClass(prefix, conf),
        getSchedulerClass(prefix, conf),
        getClientBackoffEnable(prefix, conf), maxQueueSize, prefix, conf);

    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
    this.authorize =
        conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION,
            false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);

    // Start the listener here and let it bind to the port
    listener = new Listener(port);
    // set the server port to the default listener port.
    this.port = listener.getAddress().getPort();
    connectionManager = new ConnectionManager();
    this.rpcMetrics = RpcMetrics.create(this, conf);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

    this.setLogSlowRPC(conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC,
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_DEFAULT));

    this.setPurgeIntervalNanos(conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_PURGE_INTERVAL_MINUTES_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_PURGE_INTERVAL_MINUTES_DEFAULT));

    // Create the responder here
    responder = new Responder();

    if (secretManager != null || UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
      saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
    }

    this.exceptionsHandler.addTerseLoggingExceptions(StandbyException.class);
    this.exceptionsHandler.addTerseLoggingExceptions(
        HealthCheckFailedException.class);
  }

  public synchronized void addAuxiliaryListener(int auxiliaryPort)
      throws IOException {
    if (auxiliaryListenerMap == null) {
      auxiliaryListenerMap = new HashMap<>();
    }
    if (auxiliaryListenerMap.containsKey(auxiliaryPort) && auxiliaryPort != 0) {
      throw new IOException(
          "There is already a listener binding to: " + auxiliaryPort);
    }
    Listener newListener = new Listener(auxiliaryPort);
    newListener.setIsAuxiliary();

    // in the case of port = 0, the listener would be on a != 0 port.
    LOG.info("Adding a server listener on port " +
        newListener.getAddress().getPort());
    auxiliaryListenerMap.put(newListener.getAddress().getPort(), newListener);
  }

  private RpcHeaderProtos.RpcSaslProto buildNegotiateResponse(List<SaslRpcServer.AuthMethod> authMethods)
      throws IOException {
    RpcHeaderProtos.RpcSaslProto.Builder negotiateBuilder = RpcHeaderProtos.RpcSaslProto.newBuilder();
    if (authMethods.contains(SaslRpcServer.AuthMethod.SIMPLE) && authMethods.size() == 1) {
      // SIMPLE-only servers return success in response to negotiate
      negotiateBuilder.setState(RpcHeaderProtos.RpcSaslProto.SaslState.SUCCESS);
    } else {
      negotiateBuilder.setState(RpcHeaderProtos.RpcSaslProto.SaslState.NEGOTIATE);
      for (SaslRpcServer.AuthMethod authMethod : authMethods) {
        SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);
        RpcHeaderProtos.RpcSaslProto.SaslAuth.Builder builder = negotiateBuilder.addAuthsBuilder()
            .setMethod(authMethod.toString())
            .setMechanism(saslRpcServer.mechanism);
        if (saslRpcServer.protocol != null) {
          builder.setProtocol(saslRpcServer.protocol);
        }
        if (saslRpcServer.serverId != null) {
          builder.setServerId(saslRpcServer.serverId);
        }
      }
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private List<SaslRpcServer.AuthMethod> getAuthMethods(SecretManager<?> secretManager,
                                                        Configuration conf) {
    UserGroupInformation.AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);
    List<SaslRpcServer.AuthMethod> authMethods = new ArrayList<>();
    if (confAuthenticationMethod == UserGroupInformation.AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new IllegalArgumentException(UserGroupInformation.AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      }
    } else if (secretManager != null) {
      LOG.debug(UserGroupInformation.AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(UserGroupInformation.AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());

    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }

  private void closeConnection(Connection connection) {
    connectionManager.close(connection);
  }

  /**
   * Setup response for the IPC Call.
   *
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(
      RpcCall call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto status, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto erCode,
      Writable rv, String errorClass, String error)
      throws IOException {
    // fatal responses will cause the reader to close the connection.
    if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL) {
      call.connection.setShouldClose();
    }
    RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder =
        RpcHeaderProtos.RpcResponseHeaderProto.newBuilder();
    headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
    headerBuilder.setCallId(call.callId);
    headerBuilder.setRetryCount(call.retryCount);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);
    if (alignmentContext != null) {
      alignmentContext.updateResponseState(headerBuilder);
    }

    if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS) {
      RpcHeaderProtos.RpcResponseHeaderProto header = headerBuilder.build();
      try {
        setupResponse(call, header, rv);
      } catch (Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR,
            RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      setupResponse(call, headerBuilder.build(), null);
    }
  }

  private void setupResponse(RpcCall call,
                             RpcHeaderProtos.RpcResponseHeaderProto header, Writable rv) throws IOException {
    final byte[] response;
    if (rv == null || (rv instanceof OzoneRpcWritable.ProtobufWrapper)) {
      response = setupResponseForProtobuf(header, rv);
    } else {
      response = setupResponseForWritable(header, rv);
    }
    if (response.length > maxRespSize) {
      LOG.warn("Large response size " + response.length + " for call "
          + call.toString());
    }
    call.setResponse(ByteBuffer.wrap(response));
  }

  private byte[] setupResponseForWritable(
      RpcHeaderProtos.RpcResponseHeaderProto header, Writable rv) throws IOException {
    OzoneResponseBuffer buf = responseBuffer.get().reset();
    try {
      OzoneRpcWritable.wrap(header).writeTo(buf);
      if (rv != null) {
        OzoneRpcWritable.wrap(rv).writeTo(buf);
      }
      return buf.toByteArray();
    } finally {
      // Discard a large buf and reset it back to smaller size
      // to free up heap.
      if (buf.capacity() > maxRespSize) {
        buf.setCapacity(INITIAL_RESP_BUF_SIZE);
      }
    }
  }


  // writing to a pre-allocated array is the most efficient way to construct
  // a protobuf response.
  private byte[] setupResponseForProtobuf(
      RpcHeaderProtos.RpcResponseHeaderProto header, Writable rv) throws IOException {
    Message payload = (rv != null)
        ? ((OzoneRpcWritable.ProtobufWrapper)rv).getMessage() : null;
    int length = getDelimitedLength(header);
    if (payload != null) {
      length += getDelimitedLength(payload);
    }
    byte[] buf = new byte[length + 4];
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    // the stream only supports little endian ints
    cos.writeRawByte((byte)((length >>> 24) & 0xFF));
    cos.writeRawByte((byte)((length >>> 16) & 0xFF));
    cos.writeRawByte((byte)((length >>>  8) & 0xFF));
    cos.writeRawByte((byte)((length >>>  0) & 0xFF));
    cos.writeUInt32NoTag(header.getSerializedSize());
    header.writeTo(cos);
    if (payload != null) {
      cos.writeUInt32NoTag(payload.getSerializedSize());
      payload.writeTo(cos);
    }
    return buf;
  }

  private static int getDelimitedLength(Message message) {
    int length = message.getSerializedSize();
    return length + CodedOutputStream.computeUInt32SizeNoTag(length);
  }

  /**
   * Setup response for the IPC Call on Fatal Error from a
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(ByteArrayOutputStream response,
                                            RpcCall call,
                                            Writable rv, String errorClass, String error)
      throws IOException {
    final int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }

  private void wrapWithSasl(RpcCall call) throws IOException {
    if (call.connection.saslServer != null) {
      byte[] token = call.rpcResponse.array();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      // rebuild with sasl header and payload
      RpcHeaderProtos.RpcResponseHeaderProto saslHeader = RpcHeaderProtos.RpcResponseHeaderProto.newBuilder()
          .setCallId(AuthProtocol.SASL.callId)
          .setStatus(RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS)
          .build();
      RpcHeaderProtos.RpcSaslProto saslMessage = RpcHeaderProtos.RpcSaslProto.newBuilder()
          .setState(RpcHeaderProtos.RpcSaslProto.SaslState.WRAP)
          .setToken(ByteString.copyFrom(token))
          .build();
      setupResponse(call, saslHeader, OzoneRpcWritable.wrap(saslMessage));
    }
  }

  Configuration getConf() {
    return conf;
  }

  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  public void setTracer(Tracer t) {
    this.tracer = t;
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();
    listener.start();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener newListener : auxiliaryListenerMap.values()) {
        newListener.start();
      }
    }

    handlers = new Handler[handlerCount];

    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener newListener : auxiliaryListenerMap.values()) {
        newListener.interrupt();
        newListener.doStop();
      }
    }
    responder.interrupt();
    notifyAll();
    this.rpcMetrics.shutdown();
    this.rpcDetailedMetrics.shutdown();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * Return the set of all the configured auxiliary socket addresses NameNode
   * RPC is listening on. If there are none, or it is not configured at all, an
   * empty set is returned.
   * @return the set of all the auxiliary addresses on which the
   *         RPC server is listening on.
   */
  public synchronized Set<InetSocketAddress> getAuxiliaryListenerAddresses() {
    Set<InetSocketAddress> allAddrs = new HashSet<>();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener auxListener : auxiliaryListenerMap.values()) {
        allAddrs.add(auxListener.getAddress());
      }
    }
    return allAddrs;
  }

  /**
   * Called for each call.
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }

  /** Called for each call. */
  public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
                                Writable param, long receiveTime) throws Exception;

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(UserGroupInformation user, String protocolName,
                         InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new AuthorizationException("Null protocol not authorized");
      }
      Class<?> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (ClassNotFoundException cfne) {
        throw new AuthorizationException("Unknown protocol: " +
            protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }

  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public int getPort() {
    return port;
  }

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return connectionManager.size();
  }

  /**
   * Get the NumOpenConnections/User.
   */
  public String getNumOpenConnectionsPerUser() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper
          .writeValueAsString(connectionManager.getUserToConnectionsMap());
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * The number of RPC connections dropped due to
   * too many connections.
   * @return the number of dropped rpc connections
   */
  public long getNumDroppedConnections() {
    return connectionManager.getDroppedConnections();

  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }

  public boolean isClientBackoffEnabled() {
    return callQueue.isClientBackoffEnabled();
  }

  public void setClientBackoffEnabled(boolean value) {
    callQueue.setClientBackoffEnabled(value);
  }

  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public int getNumReaders() {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private int channelWrite(WritableByteChannel channel,
                           ByteBuffer buffer) throws IOException {

    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
        channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }


  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private int channelRead(ReadableByteChannel channel,
                          ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
        channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }

  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }

  protected int getMaxIdleTime() {
    return connectionManager.maxIdleTime;
  }

  public String getServerName() {
    return serverName;
  }
}
