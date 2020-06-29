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

package org.apache.flink.hackathon;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests for the application API.
 */
public class ApplicationAPIITCase extends TestLogger {

	@Test
	public void testApplicationAPI() throws Exception {
		final ApplicationEnvironment env = ApplicationEnvironment.getEnvironment();

		final ReinforcementLearner reinforcementLearner = env.remote(ReinforcementLearner.class, ReinforcementLearnerApplication.class);

		final Future<Policy> policyFuture = reinforcementLearner.trainPolicy();

		final Policy policy = policyFuture.get();

		System.out.println(policy);
	}

	/**
	 * Policy.
	 */
	public static class Policy {

	}

	/**
	 * Simulation.
	 */
	public static class Simulation {

	}

	public interface ReinforcementLearner {
		Future<Policy> trainPolicy();

		Future<Policy> createPolicy();

		Future<Policy> updatePolicy(Future<Policy> policy, Future<Simulation> simulation);
	}

	public interface Simulator {
		Future<Simulation> simulate(Future<Policy> policy, int numberSteps);
	}

	/**
	 * Default simulator.
	 */
	public static class DefaultSimulator extends Application<Simulator> implements Simulator {

		public DefaultSimulator(ApplicationContext applicationContext) {
			super(Simulator.class, applicationContext);
		}

		@Override
		public Future<Simulation> simulate(Future<Policy> policy, int numberSteps) {
			final Policy currentPolicy;

			try {
				currentPolicy = policy.get();
			} catch (InterruptedException | ExecutionException e) {
				return FutureUtils.completedExceptionally(e);
			}

			return CompletableFuture.completedFuture(new Simulation());
		}
	}

	/**
	 * ReinforcementLearnerApplication.
	 */
	public static class ReinforcementLearnerApplication extends Application<ReinforcementLearner> implements ReinforcementLearner {

		public ReinforcementLearnerApplication(ApplicationContext applicationContext) {
			super(ReinforcementLearner.class, applicationContext);
		}

		public Future<Policy> trainPolicy() {
			System.out.println("trainPolicy");
			Future<Policy> policy = remoteTask().createPolicy();

			try {
				Thread.sleep(10000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			final Simulator simulator = remoteActor(Simulator.class, DefaultSimulator.class);

			for (int i = 0; i < 100; i++) {
				final Future<Simulation> simulation = simulator.simulate(policy, 100);

				policy = remoteTask().updatePolicy(policy, simulation);
			}

			return policy;
		}

		@Override
		public Future<Policy> createPolicy() {
			return CompletableFuture.completedFuture(new Policy());
		}

		@Override
		public Future<Policy> updatePolicy(Future<Policy> policy, Future<Simulation> simulations) {
			System.out.println("updatePolicy");
			return policy;
		}
	}
}
