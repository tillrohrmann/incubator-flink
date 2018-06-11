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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.cli.AbstractCustomCommandLine;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.cluster.KubernetesClusterDescriptor;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Kubernetes {@link CustomCommandLine} implementation.
 */
public class KubernetesCustomCommandLine extends AbstractCustomCommandLine<String> {

	static final String K8S = "k8s";
	private static final String CLUSTER_ID = "Kubernetes";

	private static final Options KUBERNETES_OPTIONS = new Options();
	private static final Option IMAGE_OPTION = new Option("i", "image", true, "Image name");
	private static final Option USER_CODE_JAR_OPTION = new Option("u", "userCodeJar", true, "User code jar");
	private static final Option ENTRY_CLASS = new Option("e", "entryClass", true, "User code jar");

	static {
		KUBERNETES_OPTIONS.addOption(IMAGE_OPTION);
		KUBERNETES_OPTIONS.addOption(USER_CODE_JAR_OPTION);
		KUBERNETES_OPTIONS.addOption(ENTRY_CLASS);
	}

	public KubernetesCustomCommandLine(Configuration configuration) {
		super(configuration);
	}

	@Override
	public boolean isActive(CommandLine commandLine) {
		final String addressOptionValue = commandLine.getOptionValue(addressOption.getOpt(), "");

		return addressOptionValue.startsWith(K8S);
	}

	@Override
	public String getId() {
		return CLUSTER_ID;
	}

	@Override
	public void addRunOptions(Options baseOptions) {
		super.addRunOptions(baseOptions);

		for (Option option : KUBERNETES_OPTIONS.getOptions()) {
			baseOptions.addOption(option);
		}
	}

	@Override
	public ClusterDescriptor<String> createClusterDescriptor(CommandLine commandLine) throws FlinkException {
		final String imageNameValue = commandLine.getOptionValue(IMAGE_OPTION.getOpt(), "flink:native-kubernetes");
		final String className = commandLine.getOptionValue(ENTRY_CLASS.getOpt());
		final String userCodeJar = commandLine.getOptionValue(USER_CODE_JAR_OPTION.getOpt());

		try {
			return new KubernetesClusterDescriptor(getConfiguration(), imageNameValue, className, userCodeJar);
		} catch (IOException e) {
			throw new FlinkException("Could not create the KubernetesClusterDescriptor.", e);
		}
	}

	@Nullable
	@Override
	public String getClusterId(CommandLine commandLine) {
		return null;
	}

	@Override
	public ClusterSpecification getClusterSpecification(CommandLine commandLine) {
		return new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();
	}
}
