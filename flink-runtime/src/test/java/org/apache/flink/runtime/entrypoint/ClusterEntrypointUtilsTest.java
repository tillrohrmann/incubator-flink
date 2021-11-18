/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/** Tests for the {@link ClusterEntrypointUtils}. */
public class ClusterEntrypointUtilsTest extends TestLogger {
    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testTmpWorkingDirectoryFolderIsDeletedUponCreation() throws IOException {
        final File workingDirBase = TEMPORARY_FOLDER.newFolder();
        final ResourceID resourceId = ResourceID.generate();

        final Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.PROCESS_WORKING_DIR_BASE, workingDirBase.getAbsolutePath());
        configuration.set(JobManagerOptions.JOB_MANAGER_RESOURCE_ID, resourceId.toString());

        createTmpFile(workingDirBase, resourceId);

        ClusterEntrypointUtils.configureJobManagerWorkingDirectory(configuration, resourceId);

        final File tmpWorkingDirectory =
                ClusterEntrypointUtils.getTmpWorkingDirectory(configuration);

        assertTrue(directoryIsEmptyOrDoesNotExist(tmpWorkingDirectory));
    }

    private boolean directoryIsEmptyOrDoesNotExist(File tmpWorkingDirectory) {
        return !tmpWorkingDirectory.exists() || tmpWorkingDirectory.list().length == 0;
    }

    private void createTmpFile(File workingDirBase, ResourceID resourceId) throws IOException {
        final File workingDirectory =
                ClusterEntrypointUtils.getWorkingDir(workingDirBase.getAbsolutePath(), resourceId);

        final File tmpWorkingDir = ClusterEntrypointUtils.getTmpWorkingDir(workingDirectory);

        assertTrue("Could not create the tmp working directory.", tmpWorkingDir.mkdirs());

        final File tmpFile = new File(tmpWorkingDir, "foobar");
        assertTrue(tmpFile.createNewFile());
        FileUtils.writeFileUtf8(tmpFile, "Foobar");
    }
}
