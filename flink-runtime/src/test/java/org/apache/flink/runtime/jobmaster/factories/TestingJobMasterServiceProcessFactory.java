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

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcess;
import org.apache.flink.runtime.jobmaster.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.TestingJobMasterServiceProcess;
import org.apache.flink.util.function.SupplierWithException;

/**
 * Testing implementation of the {@link JobMasterServiceProcessFactory} which returns a {@link
 * JobMaster} mock.
 */
public class TestingJobMasterServiceProcessFactory implements JobMasterServiceProcessFactory {

    private final SupplierWithException<JobMasterServiceProcess, Exception>
            jobMasterServiceSupplier;

    public TestingJobMasterServiceProcessFactory(
            SupplierWithException<JobMasterServiceProcess, Exception> jobMasterServiceSupplier) {
        this.jobMasterServiceSupplier = jobMasterServiceSupplier;
    }

    public TestingJobMasterServiceProcessFactory() {
        this(TestingJobMasterServiceProcess::new);
    }

    @Override
    public JobMasterServiceProcess create(
            JobGraph jobGraph,
            JobMasterId jobMasterId,
            OnCompletionActions jobCompletionActions,
            ClassLoader userCodeClassloader,
            long initializationTimestamp)
            throws Exception {
        return jobMasterServiceSupplier.get();
    }
}
