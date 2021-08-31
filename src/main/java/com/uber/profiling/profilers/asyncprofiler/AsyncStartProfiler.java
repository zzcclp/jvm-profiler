/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.profiling.profilers.asyncprofiler;

import com.uber.profiling.Arguments;
import com.uber.profiling.Profiler;
import com.uber.profiling.Reporter;
import com.uber.profiling.profilers.ProfilerBase;
import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncStartProfiler extends ProfilerBase implements Profiler {
    public final static String PROFILER_NAME = "AsyncProfiler";

    private static final AgentLogger logger = AgentLogger.getLogger(AsyncStartProfiler.class.getName());

    private long intervalMillis = 10000l;

    private AtomicBoolean isRunning = new AtomicBoolean(false);

    private Reporter reporter;

    private InfluxDB influxDB = null;
    // InfluxDB default connection properties
    private String host = "127.0.0.1";
    private String port = "4040";
    private String database = "metrics";
    private String username = "admin";
    private String password = "admin";

    private String flagMeasurement = "flag";

    private String asyncParams = "";
    private String newAsyncParams = "";
    private String asyncTag = "";
    private String newAsyncTag = "";

    public AsyncStartProfiler(Reporter reporter) {
        setReporter(reporter);
        init();
    }

    @Override
    public long getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    @Override
    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    public void setFlagMeasurement(String appId) {
        this.flagMeasurement = "flag_" + appId;
    }

    public void setAsyncParams(String asyncParams) {
        this.asyncParams = asyncParams;
    }

    @Override
    public synchronized void profile() {
        if (!ensureInfluxDBCon()) {
            return;
        }
        // if need to run async profiler
        if (checkNeedToRunAsyncProfiler()) {
            try {
                Map<String, Object> map = null;
                String dumpResultStr = null;
                if (AsyncProfiler.getInstance().getStatus()) {
                    // async profiler is running and then dump the results
                    dumpResultStr = AsyncProfiler.getInstance().execute(this.asyncParams);
                    if (StringUtils.isNotBlank(dumpResultStr)) {
                        map = new HashMap<>();
                        map.put("host", getHostName());
                        map.put("name", getProcessName());
                        map.put("processUuid", getProcessUuid());
                        map.put("appId", getAppId());

                        if (StringUtils.isNotBlank(this.asyncTag)) {
                            map.put("asyncTag", this.asyncTag);
                        }
                        if (getTag() != null) {
                            map.put("tag", getTag());
                        }
                        if (getCluster() != null) {
                            map.put("cluster", getCluster());
                        }
                        map.put("stacktrace", dumpResultStr);
                    }
                }

                // re-start async profiler
                this.asyncParams = this.newAsyncParams;
                this.asyncTag = this.newAsyncTag;
                AsyncProfiler.getInstance().start(this.asyncParams);
                isRunning.set(true);

                if (reporter != null && map != null) {
                    reporter.report(PROFILER_NAME, map);
                }
            } catch (Exception e) {
                logger.warn("Async profiler execute error: ", e);
            }
        } else {
            try {
                AsyncProfiler.getInstance().stop();
                isRunning.set(false);
            } catch (Exception e) {
                logger.warn("Executor async profiler stop error: ", e);
            }
        }
    }

    private boolean checkNeedToRunAsyncProfiler() {
        QueryResult queryResult =
                this.influxDB.query(
                        new Query("select value,asyncparams,asynctag from " + flagMeasurement, database));

        if (queryResult == null || queryResult.getResults().isEmpty()) {
            return false;
        }

        if (queryResult.getResults().get(0).getSeries() == null
                || queryResult.getResults().get(0).getSeries().isEmpty()) {
            return false;
        }

        QueryResult.Series series = queryResult.getResults().get(0).getSeries().get(0);
        if (series == null || series.getValues().isEmpty() || series.getValues().get(0).isEmpty()) {
            return false;
        }

        Double flagValue = Double.valueOf(series.getValues().get(0).get(1).toString());
        if (series.getValues().get(0).get(2) != null) {
            this.newAsyncParams =
                    series.getValues().get(0).get(2).toString().replaceAll(Arguments.ARG_ASYNC_PROFILER_SEPARATOR1,
                            ",").replaceAll(Arguments.ARG_ASYNC_PROFILER_SEPARATOR2, "=");
        }
        if (series.getValues().get(0).get(3) != null) {
            this.newAsyncTag =
                    series.getValues().get(0).get(3).toString();
        }

        if (flagValue == 0.0) {
            return false;
        }
        return true;
    }

    private synchronized void init() {
    }

    private boolean ensureInfluxDBCon() {
        if (influxDB != null) {
            return true;
        }
        try {
            String url = "http://" + host + ":" + port;
            logger.info("Trying to connect InfluxDB using url=" + url + ", database=" + database + ", username="
                    + username + ", password=" + password);
            this.influxDB = InfluxDBFactory.connect(url, username, password);
            // enable batch
            this.influxDB.enableBatch(BatchOptions.DEFAULTS);
            // set log level
            influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
        } catch (Exception e) {
            logger.warn("Connect to influxdb " + host + ":" + port + " error: ", e);
            return false;
        }
        return true;
    }

    public void updateArguments(Map<String, List<String>> connectionProperties) {
        for (Map.Entry<String,  List<String>> entry : connectionProperties.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();
            if (StringUtils.isNotEmpty(key) && value != null && !value.isEmpty()) {
                String stringValue = value.get(0);
                if (key.equals("influxdb.host")) {
                    logger.info("Got value for host = "+stringValue);
                    this.host = stringValue;
                } else if (key.equals("influxdb.port")) {
                    logger.info("Got value for port = "+stringValue);
                    this.port = stringValue;
                } else if (key.equals("influxdb.database")) {
                    logger.info("Got value for database = "+stringValue);
                    this.database = stringValue;
                } else if (key.equals("influxdb.username")) {
                    logger.info("Got value for username = "+stringValue);
                    this.username = stringValue;
                } else if (key.equals("influxdb.password")) {
                    logger.info("Got value for password = "+stringValue);
                    this.password = stringValue;
                }
            }
        }
        // connect to influxdb
        ensureInfluxDBCon();
    }

    @Override
    public void close() {
        synchronized (this) {
            this.influxDB.close();
            this.influxDB = null;
            try {
                AsyncProfiler.getInstance().stop();
            } catch (Exception e) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // InfluxDBOutputReporter  ConsoleOutputReporter
        String argumentStr = "configProvider=com.uber.profiling.util" +
                ".DummyConfigProvider,reporter=com.uber.profiling.reporters" +
                ".InfluxDBOutputReporter,tag=mytag,appIdVariable=spark31soft," +
                "metricInterval=200,durationProfiling=com.uber.profiling.examples" +
                ".HelloWorldApplication.publicSleepMethod,argumentProfiling=com.uber" +
                ".profiling.examples.HelloWorldApplication.publicSleepMethod.1," +
                "ioProfiling=true,asyncProfilerParams=event__cpu___collapsed___alluser," +
                "asyncProfilerInterval=1000,influxdb.host=mydocker,influxdb.port=13040," +
                "enabledOutputLog=true,asyncProfilerLibPath=/data1/libasyncProfiler.so";
        Arguments arguments = Arguments.parseArgs(argumentStr);
        AsyncProfiler.getInstance(arguments.getAsyncProfilerLibPath());
        AsyncStartProfiler asyncStartProfiler = new AsyncStartProfiler(arguments.getReporter());

        asyncStartProfiler.setTag(arguments.getTag());
        asyncStartProfiler.setCluster(arguments.getCluster());
        asyncStartProfiler.setIntervalMillis(arguments.getAsyncProfilerSamplingInterval());
        asyncStartProfiler.setProcessUuid("aaaa-bbbb-cccc");
        asyncStartProfiler.setAppId(arguments.getAppIdVariable());
        asyncStartProfiler.setFlagMeasurement(arguments.getAppIdVariable());
        asyncStartProfiler.setAsyncParams(arguments.getAsyncProfilerParams());
        asyncStartProfiler.updateArguments(arguments.getRawArgValues());
        asyncStartProfiler.profile();
        for (int i = 0; i < 1000; i++) {
            long cnt = 0;
            while (true) {
                Thread.sleep(10);
                cnt += 10;
                if (cnt > 30000) {
                    break;
                }
            }
            asyncStartProfiler.profile();
        }
        asyncStartProfiler.close();
    }
}
