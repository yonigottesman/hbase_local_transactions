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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Factory to create an {@link RpcRetryingCaller}
 */
@InterfaceAudience.Private
public class RpcRetryingCallerFactory {

  /** Configuration key for a custom {@link RpcRetryingCaller} */
  public static final String CUSTOM_CALLER_CONF_KEY = "hbase.rpc.callerfactory.class";
  protected final Configuration conf;
  private final long pause;
  private final int retries;
  private final int startLogErrorsCnt;
  private final boolean enableBackPressure;
  private ServerStatisticTracker stats;

  public RpcRetryingCallerFactory(Configuration conf) {
    this(conf, null);
  }

  public RpcRetryingCallerFactory(Configuration conf, ServerStatisticTracker stats) {
    this.conf = conf;
    this.stats = stats;
    pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    startLogErrorsCnt = conf.getInt(AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY,
        AsyncProcess.DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    enableBackPressure = conf.getBoolean(HConstants.ENABLE_CLIENT_BACKPRESSURE,
      HConstants.DEFAULT_ENABLE_CLIENT_BACKPRESSURE);
  }

  /**
   * Set the tracker that should be used for tracking statistics about the server
   */
  public void setStatisticTracker(ServerStatisticTracker statisticTracker) {
    this.stats = statisticTracker;
  }

  public <T> RpcRetryingCaller<T> newCaller() {
    // We store the values in the factory instance. This way, constructing new objects
    //  is cheap as it does not require parsing a complex structure.
    RpcRetryingCaller<T> caller;
    if (enableBackPressure && this.stats != null) {
      caller = new StatsTrackingRpcRetryingCaller<T>(pause, retries, startLogErrorsCnt,
        this.stats);
    } else {
      caller = new RpcRetryingCaller<T>(pause, retries, startLogErrorsCnt);
    }
    return caller;
  }

  public static RpcRetryingCallerFactory instantiate(Configuration configuration,
      ServerStatisticTracker stats) {
    String clazzName = RpcRetryingCallerFactory.class.getName();
    String rpcCallerFactoryClazz =
        configuration.get(RpcRetryingCallerFactory.CUSTOM_CALLER_CONF_KEY, clazzName);
    if (rpcCallerFactoryClazz.equals(clazzName)) {
      return new RpcRetryingCallerFactory(configuration, stats);
    }
    try {
      return ReflectionUtils.instantiateWithCustomCtor(rpcCallerFactoryClazz,
        new Class[] { Configuration.class, ServerStatisticTracker.class },
        new Object[] { configuration, stats });
    } catch (UnsupportedOperationException e) {
      return ReflectionUtils.instantiateWithCustomCtor(rpcCallerFactoryClazz,
        new Class[] { Configuration.class },
        new Object[] { configuration });
    }
  }
}
