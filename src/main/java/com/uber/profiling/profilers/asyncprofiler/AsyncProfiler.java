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

import com.uber.profiling.util.AgentLogger;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Java API for in-process profiling. Serves as a wrapper around
 * async-profiler native library. This class is a singleton.
 * The first call to {@link #getInstance()} initiates loading of
 * libasyncProfiler.so.
 */
public class AsyncProfiler {
    private static final AgentLogger logger = AgentLogger.getLogger(AsyncProfiler.class.getName());

    private static AsyncProfiler instance;

    private String currLibPath = "";

    private AsyncProfiler() {
    }

    public static AsyncProfiler getInstance() {
        return getInstance(null);
    }

    public static synchronized AsyncProfiler getInstance(String libPath) {
        if (instance != null) {
            return instance;
        }

        File file = null;
        boolean libPathExists = false;
        if (StringUtils.isNotBlank(libPath)) {
            file = new File(libPath);
            libPathExists = file.isFile() && file.exists();
        }
        if (!libPathExists) {
            String soFileName = "/libasyncProfiler.so";
            if (System.getProperty("os.name").toLowerCase().contains("mac")) {
                soFileName = "/libasyncProfiler-mac.so";
            }
            try {
                InputStream is = AsyncProfiler.class.getResourceAsStream(soFileName);
                file = File.createTempFile("lib", ".so");
                OutputStream os = new FileOutputStream(file);
                byte[] buffer = new byte[128 << 10];
                int length;
                while ((length = is.read(buffer)) != -1) {
                    os.write(buffer, 0, length);
                }
                is.close();
                os.close();
            } catch (IOException e) {
                logger.warn("Load libasyncProfiler.so error: ", e);
            }
        }
        if (file != null) {
            try {
                file.setReadable(true, false);
                logger.info("Load libasyncProfiler from path: " + file.getAbsolutePath());
                System.load(file.getAbsolutePath());
                libPath = file.getAbsolutePath();
            } catch (UnsatisfiedLinkError error) {
                logger.warn("Load libasyncProfiler.so error: ", error);
                throw error;
            }
        }
        instance = new AsyncProfiler();
        instance.setCurrLibPath(libPath);
        return instance;
    }

    /**
     * Start profiling
     *
     * @param command Profiling command
     * @param interval Sampling interval, e.g. nanoseconds for Events.CPU
     * @throws IllegalStateException If profiler is already running
     */
    public void start(String command)
            throws IllegalArgumentException, IllegalStateException, IOException {
        // start0(event, interval, true);
        StringBuffer startCommand = new StringBuffer(Actions.start.toString());
        if (StringUtils.isNotBlank(command)) {
            startCommand.append(",").append(command);
        }
        logger.info("execute command: " + startCommand);
        this.execute0(startCommand.toString());
    }

    /**
     * Start or resume profiling without resetting collected data.
     * Note that event and interval may change since the previous profiling session.
     *
     * @param command Profiling command
     * @param interval Sampling interval, e.g. nanoseconds for Events.CPU
     * @throws IllegalStateException If profiler is already running
     */
    public void resume(String command)
            throws IllegalArgumentException, IllegalStateException, IOException {
        /* if (command == null || command.isEmpty()) {
            throw new NullPointerException();
        }
        start0(event, interval, false); */
        StringBuffer startCommand = new StringBuffer(Actions.resume.toString());
        if (StringUtils.isNotBlank(command)) {
            startCommand.append(",").append(command);
        }
        logger.info("execute command: " + startCommand);
        this.execute0(startCommand.toString());
    }

    /**
     * Stop profiling (without dumping results)
     *
     */
    public void stop()
            throws IllegalArgumentException, IllegalStateException, IOException {
        if (getStatus()) {
            stop0();
        }
    }

    /**
     * Get the status of profiling
     */
    public boolean getStatus()
            throws IllegalArgumentException, IllegalStateException, IOException {
        String result = this.execute0(Actions.status.toString());
        System.out.println(result);
        return (result != null && !result.trim().equals("Profiler is not active"));
    }

    /**
     * Get the number of samples collected during the profiling session
     *
     * @return Number of samples
     */
    public native long getSamples();

    /**
     * Get profiler agent version, e.g. "1.0"
     *
     * @return Version string
     */
    public String getVersion() {
        try {
            return execute0("version");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Execute an agent-compatible profiling command -
     * the comma-separated list of arguments described in arguments.cpp
     *
     * @param command Profiling command
     * @return The command result
     * @throws IllegalArgumentException If failed to parse the command
     * @throws IOException If failed to create output file
     */
    public String execute(String command) throws IllegalArgumentException, IllegalStateException, IOException {
        if (command == null) {
            throw new NullPointerException();
        }
        logger.info("execute command: " + command);
        return execute0(command);
    }

    /**
     * Dump profile in 'collapsed stacktraces' format
     *
     * @param counter Which counter to display in the output
     * @return Textual representation of the profile
     */
    public String dumpCollapsed(Counter counter) {
        try {
            return execute0("collapsed," + counter.name().toLowerCase());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Dump collected stack traces
     *
     * @param maxTraces Maximum number of stack traces to dump. 0 means no limit
     * @return Textual representation of the profile
     */
    public String dumpTraces(int maxTraces) {
        try {
            return execute0(maxTraces == 0 ? "traces" : "traces=" + maxTraces);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Dump flat profile, i.e. the histogram of the hottest methods
     *
     * @param maxMethods Maximum number of methods to dump. 0 means no limit
     * @return Textual representation of the profile
     */
    public String dumpFlat(int maxMethods) {
        try {
            return execute0(maxMethods == 0 ? "flat" : "flat=" + maxMethods);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Add the given thread to the set of profiled threads.
     * 'filter' option must be enabled to use this method.
     *
     * @param thread Thread to include in profiling
     */
    public void addThread(Thread thread) {
        filterThread(thread, true);
    }

    /**
     * Remove the given thread from the set of profiled threads.
     * 'filter' option must be enabled to use this method.
     *
     * @param thread Thread to exclude from profiling
     */
    public void removeThread(Thread thread) {
        filterThread(thread, false);
    }

    private void filterThread(Thread thread, boolean enable) {
        if (thread == null || thread == Thread.currentThread()) {
            filterThread0(null, enable);
        } else {
            // Need to take lock to avoid race condition with a thread state change
            synchronized (thread) {
                Thread.State state = thread.getState();
                if (state != Thread.State.NEW && state != Thread.State.TERMINATED) {
                    filterThread0(thread, enable);
                }
            }
        }
    }

    public String getCurrLibPath() {
        return currLibPath;
    }

    public void setCurrLibPath(String currLibPath) {
        this.currLibPath = currLibPath;
    }

    public native void start0(String event, long interval, boolean reset) throws IllegalStateException;
    public native void stop0() throws IllegalStateException;
    public native String execute0(String command) throws IllegalArgumentException, IllegalStateException, IOException;
    public native void filterThread0(Thread thread, boolean enable);
}
