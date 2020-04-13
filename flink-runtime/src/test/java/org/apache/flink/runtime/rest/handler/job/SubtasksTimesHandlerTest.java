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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtasksTimesHeaders;
import org.apache.flink.runtime.rest.messages.SubtasksTimesInfo;
import org.apache.flink.runtime.rest.messages.SubtasksTimesMessageParameters;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests of {@link SubtasksTimesHandler}.
 */
public class SubtasksTimesHandlerTest extends TestLogger {

	private static HashMap<String, String> receivedPathParameters;
	private static SubtasksTimesHandler handler;
	private static final JobID JOBID = new JobID();
	private static final JobVertexID JOB_VERTEXID = new JobVertexID();

	@BeforeClass
	public static void setUpClass() {
		receivedPathParameters = new HashMap<>(2);
		receivedPathParameters.put(JobIDPathParameter.KEY, JOBID.toString());
		receivedPathParameters.put(JobVertexIdPathParameter.KEY, JOB_VERTEXID.toString());

		RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(new Configuration());
		handler = new SubtasksTimesHandler(
			() -> null,
			Time.milliseconds(100),
			Collections.emptyMap(),
			SubtasksTimesHeaders.getInstance(),
			new ExecutionGraphCache(
				restHandlerConfiguration.getTimeout(),
				Time.milliseconds(restHandlerConfiguration.getRefreshInterval())),
			TestingUtils.defaultExecutor());
	}

	@Test
	public void testHandleRequest() throws Exception {
		final boolean showHistory = true;
		HandlerRequest<EmptyRequestBody, SubtasksTimesMessageParameters> testRequest = new HandlerRequest<EmptyRequestBody, SubtasksTimesMessageParameters>(
			EmptyRequestBody.getInstance(),
			new SubtasksTimesMessageParameters(),
			receivedPathParameters,
			Collections.singletonMap("show-history", Collections.singletonList(""+showHistory)));
		ArchivedExecutionJobVertex executionVertex = createExecutionJobVertex();
		final SubtasksTimesInfo detailsInfo = handler.handleRequest(testRequest, executionVertex);
		SubtasksTimesInfo expectedSubtasksTimesInfo = handler.createSubtaskTimesInfo(executionVertex, showHistory);
		assertEquals(detailsInfo, expectedSubtasksTimesInfo);
	}

	private ArchivedExecutionJobVertex createExecutionJobVertex() {
		final long bytesIn = 1L;
		final long bytesOut = 10L;
		final long recordsIn = 20L;
		final long recordsOut = 30L;

		final IOMetrics ioMetrics = new IOMetrics(
			bytesIn,
			bytesOut,
			recordsIn,
			recordsOut);

		final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();

		final int subtaskIndex = 1;
		final int currentAttemptNum = 2;
		final long duration = 1024L;
		EvictingBoundedList<ArchivedExecution> priorExecutionAttempt = new EvictingBoundedList<>(currentAttemptNum);
		for (int i = 0; i < currentAttemptNum; i++) {
			long deployTs = System.currentTimeMillis() - (currentAttemptNum + 1 - i) * duration;
			priorExecutionAttempt.add(createAttempt(ioMetrics, i, ExecutionState.FAILED, assignedResourceLocation, subtaskIndex, deployTs, duration));
		}
		ArchivedExecution currentExecution = createAttempt(ioMetrics, currentAttemptNum, ExecutionState.FINISHED, assignedResourceLocation, subtaskIndex, System.currentTimeMillis() - duration, duration);

		final StringifiedAccumulatorResult[] emptyAccumulators = new StringifiedAccumulatorResult[0];
		return new ArchivedExecutionJobVertex(
			new ArchivedExecutionVertex[]{
				new ArchivedExecutionVertex(
					subtaskIndex,
					"Test archived execution vertex",
					currentExecution,
					priorExecutionAttempt)
			},
			JOB_VERTEXID,
			"test",
			1,
			1,
			ResourceProfile.UNKNOWN,
			emptyAccumulators);
	}

	private ArchivedExecution createAttempt(IOMetrics ioMetrics, int attemptNumber, ExecutionState expectedState,
			TaskManagerLocation assignedResourceLocation, int parallelSubtaskIndex, long deployTs, long duration) {
		return new ArchivedExecution(
			new StringifiedAccumulatorResult[0],
			ioMetrics,
			new ExecutionAttemptID(),
			attemptNumber,
			expectedState,
			null,
			assignedResourceLocation,
			new AllocationID(),
			parallelSubtaskIndex,
			createTimes(deployTs, duration, expectedState));
	}

	private long[] createTimes(long deployingTs, long duration, ExecutionState lastState) {
		final long[] timestamps = new long[ExecutionState.values().length];
		timestamps[ExecutionState.DEPLOYING.ordinal()] = deployingTs;
		timestamps[lastState.ordinal()] = deployingTs + duration;
		return timestamps;
	}
}
