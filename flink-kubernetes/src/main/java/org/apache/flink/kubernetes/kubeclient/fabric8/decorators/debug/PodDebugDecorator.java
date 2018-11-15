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

package org.apache.flink.kubernetes.kubeclient.fabric8.decorators.debug;

import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.fabric8.decorators.Decorator;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * A decorator to add debug option.
 */
public class PodDebugDecorator extends Decorator<Pod, FlinkPod> {

	public static final String FLINK_DIST_VOLUME_NAME = "flink-dist-volume";
	public static final String FLINK_PROJECT_VOLUME_NAME = "flink-root-volume";
	public static final String FLINK_PROJECT_VOLUME_PATH = "/flink-root/";

	@Override
	protected Pod doDecorate(Pod resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		Preconditions.checkArgument(flinkKubernetesOptions != null && flinkKubernetesOptions.getIsDebugMode());
		Preconditions.checkArgument(resource.getSpec() != null && resource.getSpec().getContainers() != null);

		PodSpec spec = resource.getSpec();
		Container container = spec.getContainers().get(0);
		List<String> args = container.getArgs();

		args.add("-D" + FlinkKubernetesOptions.DEBUG_MODE.key() + "=true");

		Volume flinkProjectVolume = new VolumeBuilder()
			.withName(FLINK_DIST_VOLUME_NAME)
			.withHostPath(new HostPathVolumeSource(
				String.format("%sbuild-target/", FLINK_PROJECT_VOLUME_PATH)
				, null))
			.build();

		Volume flinkDistVolume = new VolumeBuilder()
			.withName(FLINK_PROJECT_VOLUME_NAME)
			.withHostPath(new HostPathVolumeSource(FLINK_PROJECT_VOLUME_PATH, null))
			.build();

		if (spec.getVolumes() != null) {
			spec.setVolumes(new ArrayList<>());
		}

		List<Volume> volumes = spec.getVolumes();
		volumes.add(flinkDistVolume);
		volumes.add(flinkProjectVolume);

		if (container.getVolumeMounts() == null) {
			container.setVolumeMounts(new ArrayList<>());
		}

		List<VolumeMount> mounts = container.getVolumeMounts();
		mounts.add(new VolumeMountBuilder()
			.withName(FLINK_DIST_VOLUME_NAME)
			.withMountPath("/opt/flink")
			.build());

		mounts.add(new VolumeMountBuilder()
			.withName(FLINK_PROJECT_VOLUME_NAME)
			.withMountPath("/flink_root")
			.build());

		if (container.getEnv() == null) {
			container.setEnv(new ArrayList<>());
		}

		List<EnvVar> envs = container.getEnv();

		envs.add(new EnvVarBuilder()
		.withName("EXTRA_CLASSPATHS")
		.withValue("/flink-root/flink-kubernetes/target/classes")
		.build());

		return resource;
	}
}
