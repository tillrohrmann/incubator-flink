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

import org.apache.flink.hackathon.redis.ApplicationAPIFutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for the application API.
 */
public class ApplicationAPIITCase extends TestLogger {

	@Test
	public void testApplicationAPI() throws Exception {
		final ApplicationEnvironment env = ApplicationEnvironment.getRemoteEnvironment(
			"localhost",
			8081,
			Arrays.asList(
				new File("/Users/till/work/git/flink/flink-hackathon/target/flink-hackathon_2.11-1.12-SNAPSHOT.jar"),
				new File("/Users/till/work/git/flink/flink-hackathon/target/flink-hackathon_2.11-1.12-SNAPSHOT-tests.jar")));

//		final ApplicationEnvironment env = ApplicationEnvironment.getEnvironment();

		try {
			final ReinforcementLearner reinforcementLearner = env.remote(
				ReinforcementLearner.class,
				ReinforcementLearnerApplication.class);

			final Future<Policy> policyFuture = reinforcementLearner.trainPolicy();

			final Policy policy = policyFuture.get();

			System.out.println(policy);
		} finally {
			env.close();
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
			Future<Policy> policy = remoteTask().createPolicy();

			final Collection<Simulator> simulators = IntStream.range(0, 5)
				.mapToObj(ignored -> remoteActor(Simulator.class, DefaultSimulator.class)).collect(Collectors.toList());

			for (int i = 0; i < 10; i++) {
				final Future<Policy> currentPolicy = policy;

				final Collection<Future<Simulation>> simulationResults = simulators.stream()
					.map(simulator -> simulator.simulate(currentPolicy, 50 + ThreadLocalRandom.current().nextInt(50)))
					.collect(Collectors.toList());

				final Future<Collection<Simulation>> simulations = ApplicationAPIFutureUtils.combineAll(simulationResults);

				policy = remoteTask().updatePolicy(policy, simulations);

				System.out.printf("New policy: %s\n", getPolicy(policy));
			}

			return policy;
		}

		private Policy getPolicy(Future<Policy> policy) {
			try {
				return policy.get();
			} catch (InterruptedException | ExecutionException e) {
				ExceptionUtils.rethrow(e);
				return null;
			}
		}

		@Override
		public Future<Policy> createPolicy() {
			final Policy policy = new Policy(new double[]{0.5, 0.5});

			System.out.println(String.format("Create initial policy: %s", policy));

			return CompletableFuture.completedFuture(policy);
		}

		@Override
		public Future<Policy> updatePolicy(Future<Policy> policy, Future<Collection<Simulation>> simulations) {
			final Policy currentPolicy;
			final Collection<Simulation> currentSimulations;

			try {
				currentPolicy = policy.get();
				currentSimulations = simulations.get();
			} catch (InterruptedException | ExecutionException e) {
				return FutureUtils.completedExceptionally(e);
			}

			Collection<Observation> observations = currentSimulations.stream()
				.flatMap(simulation -> simulation.getObservations().stream())
				.collect(Collectors.toList());

			double totalReward = 0;
			double[] actionReward = new double[]{0, 0};
			for (Observation observation : observations) {
				actionReward[observation.getAction().ordinal()] += observation.getReward();
				totalReward += observation.getReward();
			}

			double[] newActions = new double[2];
			for (int i = 0; i < 2; i++) {
				newActions[i] = actionReward[i] / totalReward / 2 + currentPolicy.getActions()[i] / 2;
			}

			final Policy newPolicy = new Policy(newActions);

			return CompletableFuture.completedFuture(newPolicy);
		}
	}

	/**
	 * Default simulator.
	 */
	public static class DefaultSimulator extends Application<Simulator> implements Simulator {

		private final long id;

		public DefaultSimulator(ApplicationContext applicationContext) {
			super(Simulator.class, applicationContext);
			id = ThreadLocalRandom.current().nextLong();
		}

		@Override
		public Future<Simulation> simulate(Future<Policy> policy, int numberSteps) {
			System.out.printf("%s: Start simulating %s steps\n", getName(), numberSteps);

			final Policy currentPolicy = getPolicy(policy);

			Collection<Observation> observations = new ArrayList<>(numberSteps);

			for (int i = 0; i < numberSteps; i++) {
				observations.add(simulateStep(currentPolicy));
			}

			System.out.printf("%s: Finished simulating %s steps\n", getName(), numberSteps);

			return CompletableFuture.completedFuture(new Simulation(observations));
		}

		private Policy getPolicy(Future<Policy> policyFuture) {
			try {
				return policyFuture.get();
			} catch (InterruptedException | ExecutionException e) {
				ExceptionUtils.rethrow(e);
				return null;
			}
		}

		private Observation simulateStep(Policy policy) {
			final Action action = ThreadLocalRandom.current().nextDouble() <= policy.getActions()[0] ? Action.LEFT : Action.RIGHT;
			final double reward = ThreadLocalRandom.current().nextDouble();
			sleep();
			return new Observation(action, reward);
		}

		private void sleep() {
			try {
				Thread.sleep(ThreadLocalRandom.current().nextLong(50, 100));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public void printStats() {
			System.out.println(getName());
		}

		private String getName() {
			return String.format("DefaultSimulator(%s)", id);
		}
	}

	enum Action {
		LEFT,
		RIGHT,
	}

	/**
	 * Policy.
	 */
	public static class Policy implements Serializable {

		private static final long serialVersionUID = 8783989119806999887L;

		private final double[] actions;

		public Policy(double[] actions) {
			this.actions = actions;
		}

		public double[] getActions() {
			return actions;
		}

		@Override
		public String toString() {
			return "Policy{" +
				"actions={Left: " + actions[0] + ", Right: " + actions[1] +
				'}';
		}
	}

	/**
	 * Simulation.
	 */
	public static class Simulation implements Serializable{

		private static final long serialVersionUID = 2407311891360389324L;

		private final Collection<Observation> observations;

		public Simulation(Collection<Observation> observations) {
			this.observations = observations;
		}

		public Collection<Observation> getObservations() {
			return observations;
		}
	}

	/**
	 * Observation.
	 */
	public static class Observation implements Serializable {

		private static final long serialVersionUID = -4594687228136243749L;

		private final Action action;

		private final double reward;

		public Observation(Action action, double reward) {
			this.action = action;
			this.reward = reward;
		}

		public Action getAction() {
			return action;
		}

		public double getReward() {
			return reward;
		}
	}

	/**
	 * ReinforcementLearner.
	 */
	public interface ReinforcementLearner {
		Future<Policy> trainPolicy();

		Future<Policy> createPolicy();

		Future<Policy> updatePolicy(Future<Policy> policy, Future<Collection<Simulation>> simulation);
	}

	/**
	 * Simulator.
	 */
	public interface Simulator {
		Future<Simulation> simulate(Future<Policy> policy, int numberSteps);

		void printStats();
	}
}
