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

package org.apache.flink.runtime.scheduler.adaptive.scalingpolicy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link ScaleUpDownController}. */
public class ScaleUpControllerTest extends TestLogger {
    private static final Configuration TEST_CONFIG = new Configuration();

    static {
        TEST_CONFIG.set(JobManagerOptions.MIN_PARALLELISM_INCREASE, 2);
    }

    @Test
    public void testScaleUp() {
        ScaleUpDownController suc = new CumulativeParallelismScaleUpDownController(TEST_CONFIG);
        assertThat(suc.canScale(1, 4), is(true));
    }

    @Test
    public void testNoScaleUp() {
        ScaleUpDownController suc = new CumulativeParallelismScaleUpDownController(TEST_CONFIG);
        assertThat(suc.canScale(2, 3), is(false));
    }
}
