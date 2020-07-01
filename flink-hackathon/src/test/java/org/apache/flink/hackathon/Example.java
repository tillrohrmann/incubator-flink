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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Example.
 */
public class Example {

	/**
	 * ReinforcementLearnerApplication.
	 */
	public static class ReinforcementLearnerApplication {

		public Policy trainPolicy() {
			Policy policy = createPolicy();

			final Collection<DefaultSimulator> simulators = IntStream.range(0, 5)
				.mapToObj(ignored -> new DefaultSimulator()).collect(Collectors.toList());

			for (int i = 0; i < 10; i++) {
				final Policy currentPolicy = policy;

				final Collection<Simulation> simulationResults = simulators.stream()
					.map(simulator -> simulator.simulate(currentPolicy, 50 + ThreadLocalRandom.current().nextInt(50)))
					.collect(Collectors.toList());

				policy = updatePolicy(policy, simulationResults);

				System.out.printf("New policy: %s\n", policy);
			}

			return policy;
		}

		public Policy createPolicy() {
			final Policy policy = new Policy(new double[]{0.5, 0.5});

			System.out.println(String.format("Create initial policy: %s", policy));

			return policy;
		}

		public Policy updatePolicy(Policy policy, Collection<Simulation> simulations) {

			Collection<Observation> observations = simulations.stream()
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
				newActions[i] = actionReward[i] / totalReward / 2 + policy.getActions()[i] / 2;
			}

			final Policy newPolicy = new Policy(newActions);

			return newPolicy;
		}
	}

	/**
	 * Default simulator.
	 */
	public static class DefaultSimulator {

		private final long id;

		public DefaultSimulator() {
			id = ThreadLocalRandom.current().nextLong();
		}

		public Simulation simulate(Policy policy, int numberSteps) {
			System.out.printf("%s: Start simulating %s steps\n", getName(), numberSteps);

			final Policy currentPolicy = policy;

			Collection<Observation> observations = new ArrayList<>(numberSteps);

			for (int i = 0; i < numberSteps; i++) {
				observations.add(simulateStep(currentPolicy));
			}

			System.out.printf("%s: Finished simulating %s steps\n", getName(), numberSteps);

			return new Simulation(observations);
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
}
