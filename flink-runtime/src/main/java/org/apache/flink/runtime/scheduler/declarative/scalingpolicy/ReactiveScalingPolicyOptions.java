/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative.scalingpolicy;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for the {@link ReactiveScalingPolicy}. */
public final class ReactiveScalingPolicyOptions {

    @Documentation.Section({
        Documentation.Sections.EXPERT_SCHEDULING,
        Documentation.Sections.ALL_JOB_MANAGER
    })
    public static final ConfigOption<Integer> MIN_PARALLELISM_INCREASE =
            key("jobmanager.declarative-scheduler.autoscaling.min-absolute-parallelism-increase")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "Configure the minimum increase in parallelism for a job to scale up.");
}
