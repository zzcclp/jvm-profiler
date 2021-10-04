/*
 * Copyright 2020 Andrei Pangin
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

package com.uber.profiling.export;

import com.uber.profiling.flamegraph.FlameGraph;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ExportFlameBak {

    private final static long MILLSEC2NANOSEC = 1000000l;
    public FlameGraph flameGraph = null;
    public String title = "Flame Graph";
    public boolean reverse = false;
    public double minwidth = 0.0;
    public int skip = 0;
    public String input;
    public String output;
    public boolean isUberData = false;

    public InfluxDB influxDB = null;
    public String influxdbHost = null;
    public String influxdbPort = null;
    public String influxdbDatabase = "metrics";
    public String influxdbUser = "admin";
    public String influxdbPwd = "admin";
    public int influxdbChunk = 50;

    public String processIdTag;
    public String asyncTag;

    public ExportFlameBak() {
    }

    public static void main(String[] args) throws IOException {

        // example
        /* args = new String[16];
        args[0] = "--title";
        args[1] = "Kylin Profiler FlameGraph";
        args[2] = "/data1/uber-jvm-profiler/kylin-profiler/flamegraph-test.log";
        args[3] = "/data1/uber-jvm-profiler/kylin-profiler/flamegraph-test.html";

        args[4] = "--influxdbHost";
        args[5] = "mydocker";
        args[6] = "--influxdbPort";
        args[7] = "13040";
        args[8] = "--influxdbDatabase";
        args[9] = "metrics";
        args[10] = "--influxdbChunk";
        args[11] = "50";

        args[12] = "--processIdTag";
        args[13] = "kylin4_test_driver";
        args[14] = "--asyncTag";
        args[15] = "cpu-10"; */

        ExportFlameBak exportFlame = new ExportFlameBak();
        exportFlame.parseArgs(args);
        if (StringUtils.isBlank(exportFlame.influxdbHost)
                || StringUtils.isBlank(exportFlame.influxdbPort)
                || StringUtils.isBlank(exportFlame.influxdbDatabase)
                || StringUtils.isBlank(exportFlame.processIdTag)
                || StringUtils.isBlank(exportFlame.asyncTag)
                || StringUtils.isBlank(exportFlame.input)) {
            System.out.println("Usage: java " + ExportFlameBak.class.getName() + " options input.collapsed [output.html]");
            System.out.println();
            System.out.println("Options:");
            System.out.println("  --title TITLE");
            System.out.println("  --reverse");
            System.out.println("  --minwidth PERCENT");
            System.out.println("  --skip FRAMES");
            System.out.println("  --uber true or false, default is false");
            System.out.println("  --influxdbHost the host of influxdb server");
            System.out.println("  --influxdbPort the port of influxdb server");
            System.out.println("  --influxdbDatabase the target database of influxdb");
            System.out.println("  --influxdbUser the user name of influxdb");
            System.out.println("  --influxdbPwd the password of influxdb");
            System.out.println("  --influxdbChunk default value is 50");
            System.out.println("  --processIdTag process id tag");
            System.out.println("  --asyncTag async tag");
            System.exit(1);
        }
        // export data from influxdb
        exportFlame.exportDataFromInfluxdb();

        exportFlame.flameGraph = new FlameGraph();
        exportFlame.flameGraph.title = exportFlame.title;
        exportFlame.flameGraph.reverse = exportFlame.reverse;
        exportFlame.flameGraph.minwidth = exportFlame.minwidth;
        exportFlame.flameGraph.skip = exportFlame.skip;
        exportFlame.flameGraph.input = exportFlame.input;
        exportFlame.flameGraph.output = exportFlame.output;
        exportFlame.flameGraph.isUberData = exportFlame.isUberData;
        System.out.println("Start to generate flame graph.");
        exportFlame.flameGraph.parseAndDump();
    }

    public void parseArgs(String... args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--") && !arg.isEmpty()) {
                if (input == null) {
                    input = arg;
                } else {
                    output = arg;
                }
            } else if (arg.equals("--title")) {
                title = args[++i];
            } else if (arg.equals("--influxdbHost")) {
                influxdbHost = args[++i];
            } else if (arg.equals("--influxdbPort")) {
                influxdbPort = args[++i];
            } else if (arg.equals("--influxdbDatabase")) {
                influxdbDatabase = args[++i];
            } else if (arg.equals("--influxdbUser")) {
                influxdbUser = args[++i];
            } else if (arg.equals("--influxdbPwd")) {
                influxdbPwd = args[++i];
            } else if (arg.equals("--processIdTag")) {
                processIdTag = args[++i];
            } else if (arg.equals("--asyncTag")) {
                asyncTag = args[++i];
            } else if (arg.equals("--influxdbChunk")) {
                influxdbChunk = Integer.parseInt(args[++i]);
            } else if (arg.equals("--reverse")) {
                reverse = true;
            } else if (arg.equals("--minwidth")) {
                minwidth = Double.parseDouble(args[++i]);
            } else if (arg.equals("--skip")) {
                skip = Integer.parseInt(args[++i]);
            } else if (arg.equals("--uber")) {
                isUberData = Boolean.parseBoolean(args[++i]);
            }
        }
    }

    public void exportDataFromInfluxdb() throws IOException {
        // init influxdb
        String url = "http://" + influxdbHost + ":" + influxdbPort;
        this.influxDB = InfluxDBFactory.connect(url, influxdbUser, influxdbPwd)
                .enableGzip().setLogLevel(InfluxDB.LogLevel.NONE);

        // check and create data file
        File dataFile = new File(input);
        if (dataFile.exists()) {
            dataFile.delete();
        }
        dataFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(dataFile);
        BufferedWriter dataWriter = new BufferedWriter(new OutputStreamWriter(fos), 131072);

        // read data from influxdb by chunk
        final String firstTimeSql = "select first(stacktrace) from AsyncProfiler where " +
                "processIdTag = '" + processIdTag + "' and asyncTag='" + asyncTag + "'";
        final String lastTimeSql = "select last(stacktrace) from AsyncProfiler where " +
                "processIdTag = '" + processIdTag + "' and asyncTag='" + asyncTag + "'";
        final String recordCountSql = "select count(stacktrace) from AsyncProfiler where processIdTag " +
                "= '" + processIdTag + "' and asyncTag='" + asyncTag + "'";

        QueryResult queryResult = this.influxDB.query(new Query(firstTimeSql, influxdbDatabase),
                TimeUnit.MILLISECONDS);
        if (queryResult.getResults() == null || queryResult.getResults().get(0).getSeries() == null) {
            System.out.println("There is no data.");
            System.exit(1);
        }
        QueryResult queryResult1 = this.influxDB.query(new Query(lastTimeSql, influxdbDatabase),
                TimeUnit.MILLISECONDS);
        if (queryResult1.getResults() == null || queryResult1.getResults().get(0).getSeries() == null) {
            System.out.println("There is no data.");
            System.exit(1);
        }
        QueryResult queryResult2 = this.influxDB.query(new Query(recordCountSql, influxdbDatabase),
                TimeUnit.MILLISECONDS);
        if (queryResult2.getResults() == null || queryResult2.getResults().get(0).getSeries() == null) {
            System.out.println("There is no data.");
            System.exit(1);
        }

        long startTime =
                Double.valueOf(queryResult.getResults().get(0)
                        .getSeries().get(0).getValues().get(0).get(0).toString()).longValue();
        long endTime =
                Double.valueOf(queryResult1.getResults().get(0)
                        .getSeries().get(0).getValues().get(0).get(0).toString()).longValue();
        long recordCount =
                Double.valueOf(queryResult2.getResults().get(0)
                        .getSeries().get(0).getValues().get(0).get(1).toString()).longValue();

        int intervalSize = (recordCount % influxdbChunk) != 0L ?
                (int)(recordCount / influxdbChunk + 1) :
                (int)(recordCount / influxdbChunk);
        long intervalValue = (endTime - startTime) / intervalSize;
        long[] intervalArr = new long[intervalSize + 1];
        int i = 0;
        for (; i < intervalSize; i++) {
            intervalArr[i] = startTime + i * intervalValue;
        }
        intervalArr[i] = endTime + 1;
        String resultSqlFormat = "select stacktrace from AsyncProfiler where processIdTag = " +
                "'%s' and asyncTag='%s' and time >= %d and time < %d";
        for (i = 0; i < intervalSize; i++) {
            String resultSql = String.format(resultSqlFormat, processIdTag, asyncTag,
                            intervalArr[i] * MILLSEC2NANOSEC,
                            intervalArr[i + 1] * MILLSEC2NANOSEC);
            QueryResult queryResult3 = this.influxDB.query(
                    new Query(resultSql, influxdbDatabase));
            if (queryResult3.getResults() != null && queryResult3.getResults().get(0).getSeries() != null) {
                List<List<Object>> values =
                        queryResult3.getResults().get(0).getSeries().get(0).getValues();
                System.out.println("Fetch data size: " + values.size() + " by time " +
                        intervalArr[i] + " and " + intervalArr[i + 1]);
                for (List<Object> value : values) {
                    dataWriter.write(value.get(1).toString());
                }
            }
        }
        /* this.influxDB.query(
                new Query(influxdbSql, influxdbDatabase), influxdbChunk,
                queryResult -> {
                    System.out.println("result = " +
                            (queryResult.getResults() != null ?
                                    queryResult.getResults().get(0).getSeries().get(0).getValues().size() :
                            "null"));
                },
                () -> {
                    System.out.println("The query successfully finished.");
                });*/

        if (this.influxDB != null) {
            this.influxDB.close();
        }
        dataWriter.flush();
        fos.close();
        dataWriter.close();
    }
}
