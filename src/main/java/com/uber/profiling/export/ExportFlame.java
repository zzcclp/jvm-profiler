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

import okhttp3.OkHttpClient;
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

public class ExportFlame {

    public String output;

    public InfluxDB influxDB = null;
    public String influxdbHost = null;
    public String influxdbPort = null;
    public String influxdbDatabase = "metrics";
    public String influxdbUser = "admin";
    public String influxdbPwd = "admin";

    public String processIdTag;
    public String asyncTag;

    public ExportFlame() {
    }

    public static void main(String[] args) throws IOException {
        ExportFlame exportFlame = new ExportFlame();
        exportFlame.parseArgs(args);
        if (StringUtils.isBlank(exportFlame.influxdbHost)
                || StringUtils.isBlank(exportFlame.influxdbPort)
                || StringUtils.isBlank(exportFlame.influxdbDatabase)
                || StringUtils.isBlank(exportFlame.processIdTag)
                || StringUtils.isBlank(exportFlame.asyncTag)
                || StringUtils.isBlank(exportFlame.output)) {
            System.out.println("Usage: java " + ExportFlame.class.getName() + " options output.html");
            System.out.println();
            System.out.println("Options:");
            System.out.println("  --influxdbHost the host of influxdb server");
            System.out.println("  --influxdbPort the port of influxdb server");
            System.out.println("  --influxdbDatabase the target database of influxdb");
            System.out.println("  --influxdbUser the user name of influxdb");
            System.out.println("  --influxdbPwd the password of influxdb");
            System.out.println("  --processIdTag process id tag");
            System.out.println("  --asyncTag async tag");
            System.exit(1);
        }
        // export data from influxdb
        exportFlame.exportDataFromInfluxdb();
    }

    public void parseArgs(String... args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--") && !arg.isEmpty()) {
                output = arg;
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
            }
        }
    }

    public void exportDataFromInfluxdb() throws IOException {
        // init influxdb
        String url = "http://" + influxdbHost + ":" + influxdbPort;
        OkHttpClient.Builder clientBuilder = (new OkHttpClient.Builder())
                .connectTimeout(1, TimeUnit.MINUTES)
                .readTimeout(1, TimeUnit.MINUTES)
                .writeTimeout(1, TimeUnit.MINUTES)
                .retryOnConnectionFailure(true);
        this.influxDB = InfluxDBFactory.connect(url, influxdbUser, influxdbPwd, clientBuilder)
                .enableGzip().setLogLevel(InfluxDB.LogLevel.NONE);

        // check and create data file
        File dataFile = new File(output);
        if (dataFile.exists()) {
            dataFile.delete();
        }
        dataFile.createNewFile();
        FileOutputStream fos = new FileOutputStream(dataFile);
        BufferedWriter dataWriter = new BufferedWriter(new OutputStreamWriter(fos), 131072);
        String resultSqlFormat = "select stacktrace from AsyncProfiler where processIdTag = " +
                "'%s' and asyncTag='%s'";
        String resultSql = String.format(resultSqlFormat, processIdTag, asyncTag);
        QueryResult queryResult = this.influxDB.query(new Query(resultSql, influxdbDatabase));
        if (queryResult.getResults() != null && queryResult.getResults().get(0).getSeries() != null) {
            List<List<Object>> values =
                    queryResult.getResults().get(0).getSeries().get(0).getValues();
            for (List<Object> value : values) {
                dataWriter.write(value.get(1).toString());
            }
        }
        if (this.influxDB != null) {
            this.influxDB.close();
        }
        dataWriter.flush();
        fos.close();
        dataWriter.close();
    }
}
