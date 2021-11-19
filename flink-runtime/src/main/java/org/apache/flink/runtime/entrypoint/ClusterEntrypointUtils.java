/*
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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ClusterOptionsInternal;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.entrypoint.parser.ParserResultFactory;
import org.apache.flink.runtime.util.ClusterUncaughtExceptionHandler;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/** Utility class for {@link org.apache.flink.runtime.entrypoint.ClusterEntrypoint}. */
public final class ClusterEntrypointUtils {

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointUtils.class);

    private ClusterEntrypointUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }

    /**
     * Parses passed String array using the parameter definitions of the passed {@code
     * ParserResultFactory}. The method will call {@code System.exit} and print the usage
     * information to stdout in case of a parsing error.
     *
     * @param args The String array that shall be parsed.
     * @param parserResultFactory The {@code ParserResultFactory} that collects the parameter
     *     parsing instructions.
     * @param mainClass The main class initiating the parameter parsing.
     * @param <T> The parsing result type.
     * @return The parsing result.
     */
    public static <T> T parseParametersOrExit(
            String[] args, ParserResultFactory<T> parserResultFactory, Class<?> mainClass) {
        final CommandLineParser<T> commandLineParser = new CommandLineParser<>(parserResultFactory);

        try {
            return commandLineParser.parse(args);
        } catch (Exception e) {
            LOG.error("Could not parse command line arguments {}.", args, e);
            commandLineParser.printHelp(mainClass.getSimpleName());
            System.exit(ClusterEntrypoint.STARTUP_FAILURE_RETURN_CODE);
        }

        return null;
    }

    /**
     * Tries to find the user library directory.
     *
     * @return the user library directory if it exits, returns {@link Optional#empty()} if there is
     *     none
     */
    public static Optional<File> tryFindUserLibDirectory() {
        final File flinkHomeDirectory = deriveFlinkHomeDirectoryFromLibDirectory();
        final File usrLibDirectory =
                new File(flinkHomeDirectory, ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);

        if (!usrLibDirectory.isDirectory()) {
            return Optional.empty();
        }
        return Optional.of(usrLibDirectory);
    }

    @Nullable
    private static File deriveFlinkHomeDirectoryFromLibDirectory() {
        final String libDirectory = System.getenv().get(ConfigConstants.ENV_FLINK_LIB_DIR);

        if (libDirectory == null) {
            return null;
        } else {
            return new File(libDirectory).getParentFile();
        }
    }

    /**
     * Gets and verify the io-executor pool size based on configuration.
     *
     * @param config The configuration to read.
     * @return The legal io-executor pool size.
     */
    public static int getPoolSize(Configuration config) {
        final int poolSize =
                config.getInteger(
                        ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE,
                        4 * Hardware.getNumberCPUCores());
        Preconditions.checkArgument(
                poolSize > 0,
                String.format(
                        "Illegal pool size (%s) of io-executor, please re-configure '%s'.",
                        poolSize, ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE.key()));
        return poolSize;
    }

    /**
     * Sets the uncaught exception handler for current thread based on configuration.
     *
     * @param config the configuration to read.
     */
    public static void configureUncaughtExceptionHandler(Configuration config) {
        Thread.setDefaultUncaughtExceptionHandler(
                new ClusterUncaughtExceptionHandler(
                        config.get(ClusterOptions.UNCAUGHT_EXCEPTION_HANDLING)));
    }

    /**
     * This method configures the working directory of the TaskManager process identified by the
     * given resourceId.
     *
     * @param configuration to write the working directory to
     * @param resourceId identifying the process
     */
    public static void configureTaskManagerWorkingDirectory(
            Configuration configuration, ResourceID resourceId) throws IOException {
        configureProcessWorkingDirectory(
                configuration, resourceId, ClusterOptions.TASK_MANAGER_PROCESS_WORKING_DIR_BASE);
    }

    /**
     * This method configures the working directory of the JobManager process identified by the
     * given resourceId.
     *
     * @param configuration to write the working directory to
     * @param resourceId identifying the process
     */
    public static void configureJobManagerWorkingDirectory(
            Configuration configuration, ResourceID resourceId) throws IOException {
        configureProcessWorkingDirectory(
                configuration, resourceId, ClusterOptions.JOB_MANAGER_PROCESS_WORKING_DIR_BASE);
    }

    private static void configureProcessWorkingDirectory(
            Configuration configuration,
            ResourceID resourceId,
            ConfigOption<String> precedingOption)
            throws IOException {
        Preconditions.checkState(
                !configuration.contains(ClusterOptionsInternal.WORKING_DIR),
                "The working directory must not be configured directly. Instead configure %s.",
                ClusterOptions.PROCESS_WORKING_DIR_BASE);

        final String workingDirectoryBase;

        if (configuration.contains(precedingOption)) {
            workingDirectoryBase = configuration.get(precedingOption);
        } else {
            workingDirectoryBase = configuration.get(ClusterOptions.PROCESS_WORKING_DIR_BASE);
        }

        final File workingDir = getWorkingDir(workingDirectoryBase, resourceId);

        LOG.debug("Create working directory for process {} under {}.", resourceId, workingDir);

        if (!workingDir.mkdirs() && !workingDir.exists()) {
            throw new IOException(
                    String.format(
                            "Could not create the working directory %s.",
                            workingDir.getAbsolutePath()));
        }

        deleteTmpWorkingDirectory(workingDir);

        configuration.set(ClusterOptionsInternal.WORKING_DIR, workingDir.getAbsolutePath());
    }

    private static void deleteTmpWorkingDirectory(File workingDir) throws IOException {
        FileUtils.deleteDirectory(getTmpWorkingDir(workingDir));
    }

    @Nonnull
    @VisibleForTesting
    static File getTmpWorkingDir(File workingDir) {
        return new File(workingDir, "tmp");
    }

    public static File getWorkingDirectory(ReadableConfig configuration) {
        return new File(
                Preconditions.checkNotNull(
                        configuration.get(ClusterOptionsInternal.WORKING_DIR),
                        "The working directory has not been configured properly."));
    }

    public static File getTmpWorkingDirectory(ReadableConfig configuration) {
        return getTmpWorkingDir(getWorkingDirectory(configuration));
    }

    public static File getBlobsWorkingDirectory(ReadableConfig configuration) {
        return new File(getWorkingDirectory(configuration), "blobs");
    }

    @Nonnull
    public static File getLocalStateWorkingDirectory(Configuration configuration) {
        final File workingDirectory = getWorkingDirectory(configuration);

        final File localStateDir = new File(workingDirectory, "localState");
        return localStateDir;
    }

    public static File getSlotAllocationsWorkingDirectory(ReadableConfig configuration) {
        return new File(getWorkingDirectory(configuration), "slotAllocations");
    }

    @VisibleForTesting
    public static File getWorkingDir(String basePath, ResourceID resourceId) {
        return new File(basePath, resourceId.toString());
    }

    public static File getJobWorkingDirectory(JobID jobId, ReadableConfig configuration) {
        final File workingDirectory = getWorkingDirectory(configuration);
        return new File(workingDirectory, jobId.toString());
    }
}
