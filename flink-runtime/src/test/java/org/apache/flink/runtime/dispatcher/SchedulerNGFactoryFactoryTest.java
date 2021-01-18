/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.declarative.DeclarativeSchedulerFactory;
import org.apache.flink.util.TestLogger;

import org.hamcrest.core.IsNot;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link SchedulerNGFactory}. */
public class SchedulerNGFactoryFactoryTest extends TestLogger {

    private final JobGraph testGraph = new JobGraph("test");

    @Test
    public void createDefaultSchedulerFactoryByDefault() {
        final SchedulerNGFactory schedulerNGFactory =
                createSchedulerNGFactory(new Configuration(), testGraph);
        assertThat(schedulerNGFactory, is(instanceOf(DefaultSchedulerFactory.class)));
    }

    @Test
    public void createSchedulerNGFactoryIfConfigured() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Ng);

        final SchedulerNGFactory schedulerNGFactory =
                createSchedulerNGFactory(configuration, testGraph);

        assertThat(schedulerNGFactory, is(instanceOf(DefaultSchedulerFactory.class)));
    }

    @Test
    public void throwsExceptionIfSchedulerNameIsInvalid() {
        final Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.SCHEDULER.key(), "invalid-scheduler-name");

        try {
            createSchedulerNGFactory(configuration, testGraph);
        } catch (IllegalArgumentException e) {
            assertThat(
                    e.getMessage(),
                    containsString("Could not parse value 'invalid-scheduler-name'"));
        }
    }

    @Test
    public void fallBackIfBatchAndDeclarative() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Declarative);
        final JobGraph jobGraph = new JobGraph("test");
        jobGraph.setJobType(JobType.BATCH);
        final SchedulerNGFactory schedulerNGFactory =
                createSchedulerNGFactory(configuration, testGraph);

        assertThat(
                schedulerNGFactory, is(IsNot.not(instanceOf(DeclarativeSchedulerFactory.class))));
    }

    private static SchedulerNGFactory createSchedulerNGFactory(
            final Configuration configuration, JobGraph jobGraph) {
        return SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration, jobGraph);
    }
}
