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

package org.apache.flink.streaming.api.rescaling;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.rescaling.OperatorRescalingPolicy;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CompletableFuture;

/**
 * Integration test for the {@link WithRescalingPolicy} interface.
 */
public class WithRescalingPolicyTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testSimpleWithRescalingPolicy() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);
		env.enableCheckpointing(500L);
		env.setStateBackend(new MemoryStateBackend(null, "file://" + temporaryFolder.newFolder().getAbsolutePath()));

		final RescalableSource source = new RescalableSource();
		env.addSource(source)
			.setMaxParallelism(4)
			.print();

		env.execute();
	}

	private static final class RescalableSource implements ParallelSourceFunction<Integer>, WithRescalingPolicy {

		private static final long serialVersionUID = 8988310132825408432L;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(42);
				}

				Thread.sleep(50L);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public OperatorRescalingPolicy createOperatorRescalingPolicy(
			OperatorRescalingPolicy.OperatorRescalingContext operatorRescalingContext, ScheduledExecutor scheduledExecutor) throws Exception {
			return new FixedRescalingPolicy(scheduledExecutor, 2);
		}
	}

	private static class FixedRescalingPolicy implements OperatorRescalingPolicy {

		public FixedRescalingPolicy(
				ScheduledExecutor scheduledExecutor,
				int newParallelism) {
		}

		@Override
		public int rescaleTo(OperatorRescalingContext rescalingContext) {
			return 0;
		}

		@Override
		public CompletableFuture<Void> closeAsync() {
			return null;
		}
	}
}
