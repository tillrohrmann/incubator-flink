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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;
import org.mockito.Matchers;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SimpleSlotTest {

	@Test
	public void testStateTransitions() {
		try {
			// release immediately
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.isAlive());

				slot.releaseSlot();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertTrue(slot.isReleased());
			}

			// state transitions manually
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.isAlive());

				slot.markCancelled();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertFalse(slot.isReleased());

				slot.markReleased();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertTrue(slot.isReleased());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSetExecutionVertex() {
		try {
			Execution ev = mock(Execution.class);
			Execution ev_2 = mock(Execution.class);

			// assign to alive slot
			{
				SimpleSlot slot = getSlot();

				assertTrue(slot.setExecutedVertex(ev));
				assertEquals(ev, slot.getExecutedVertex());

				// try to add another one
				assertFalse(slot.setExecutedVertex(ev_2));
				assertEquals(ev, slot.getExecutedVertex());
			}

			// assign to canceled slot
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.markCancelled());

				assertFalse(slot.setExecutedVertex(ev));
				assertNull(slot.getExecutedVertex());
			}

			// assign to released marked slot
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.markCancelled());
				assertTrue(slot.markReleased());

				assertFalse(slot.setExecutedVertex(ev));
				assertNull(slot.getExecutedVertex());
			}
			
			// assign to released
			{
				SimpleSlot slot = getSlot();
				slot.releaseSlot();

				assertFalse(slot.setExecutedVertex(ev));
				assertNull(slot.getExecutedVertex());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testReleaseCancelsVertex() {
		try {
			Execution ev = mock(Execution.class);

			SimpleSlot slot = getSlot();
			assertTrue(slot.setExecutedVertex(ev));
			assertEquals(ev, slot.getExecutedVertex());

			slot.releaseSlot();
			slot.releaseSlot();
			slot.releaseSlot();

			verify(ev, times(1)).fail(Matchers.any(Throwable.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public static SimpleSlot getSlot() throws Exception {
		ResourceID resourceID = ResourceID.generate();
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		TaskManagerLocation connection = new TaskManagerLocation(resourceID, address, 10001);

		Instance instance = new Instance(
			new ActorTaskManagerGateway(DummyActorGateway.INSTANCE),
			connection,
			new InstanceID(),
			hardwareDescription,
			1);

		return instance.allocateSimpleSlot();
	}
}
