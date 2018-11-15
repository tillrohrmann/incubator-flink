package org.apache.flink.kubernetes;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.mockwebserver.Context;
import io.fabric8.mockwebserver.ServerRequest;
import io.fabric8.mockwebserver.ServerResponse;
import io.fabric8.mockwebserver.dsl.MockServerExpectation;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.rules.ExternalResource;

import java.util.HashMap;
import java.util.Queue;

/**
 * The mock server that host MixedDispatcher.
 * */
public class MixedMockKubernetesServer extends ExternalResource {

	private KubernetesMockServer mock;
	private NamespacedKubernetesClient client;
	private boolean https;
	// In this mode the mock web server will store, read, update and delete
	// kubernetes resources using an in memory map and will appear as a real api
	// server.
	private boolean crudMode;

	public MixedMockKubernetesServer(boolean https, boolean crudMode) {
		this.https = https;
		this.crudMode = crudMode;
	}

	public void before() {
		HashMap<ServerRequest, Queue<ServerResponse>> response = new HashMap<>();
		mock = crudMode
			? new KubernetesMockServer(new Context(), new MockWebServer(), response, new MixedDispatcher(response), true)
			: new KubernetesMockServer(https);
		mock.init();
		client = mock.createClient();
	}

	public void after() {
		mock.destroy();
		client.close();
	}

	public NamespacedKubernetesClient getClient() {
		return client;
	}

	public MockServerExpectation expect() {
		return mock.expect();
	}
}
