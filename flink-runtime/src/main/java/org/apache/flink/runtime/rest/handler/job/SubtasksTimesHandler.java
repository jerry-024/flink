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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.SubtasksTimesInfo;
import org.apache.flink.runtime.rest.messages.SubtasksTimesMessageParameters;
import org.apache.flink.runtime.rest.messages.SubtasksTimesShowHistoryQueryParameter;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler for the subtasks times info.
 */
public class SubtasksTimesHandler extends AbstractJobVertexHandler<SubtasksTimesInfo, SubtasksTimesMessageParameters> implements JsonArchivist {

	static final boolean SHOW_HISTORY = false;

	public SubtasksTimesHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, SubtasksTimesInfo, SubtasksTimesMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected SubtasksTimesInfo handleRequest(HandlerRequest<EmptyRequestBody, SubtasksTimesMessageParameters> request, AccessExecutionJobVertex jobVertex) {
		List<Boolean> queryParams = request.getQueryParameter(SubtasksTimesShowHistoryQueryParameter.class);

		final boolean showHistoryValue;
		if (!queryParams.isEmpty()) {
			showHistoryValue = queryParams.get(0);
		} else {
			showHistoryValue = SHOW_HISTORY;
		}
		return createSubtaskTimesInfo(jobVertex, showHistoryValue);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		Collection<? extends AccessExecutionJobVertex> allVertices = graph.getAllVertices().values();
		List<ArchivedJson> archive = new ArrayList<>(allVertices.size());
		for (AccessExecutionJobVertex task : allVertices) {
			ResponseBody json = createSubtaskTimesInfo(task, SHOW_HISTORY);
			String path = getMessageHeaders().getTargetRestEndpointURL()
				.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
				.replace(':' + JobVertexIdPathParameter.KEY, task.getJobVertexId().toString());
			archive.add(new ArchivedJson(path, json));
		}
		return archive;
	}

	@VisibleForTesting
	protected static SubtasksTimesInfo createSubtaskTimesInfo(AccessExecutionJobVertex jobVertex, boolean showHistory) {
		final String id = jobVertex.getJobVertexId().toString();
		final String name = jobVertex.getName();
		final long now = System.currentTimeMillis();
		final List<SubtasksTimesInfo.SubtaskTimeInfo> subtasks = new ArrayList<>();

		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			subtasks.add(createSubtaskTimeInfoFromAccessExecution(vertex.getCurrentExecutionAttempt(), now));
			if (showHistory) {
				vertex.getPriorExecutionAttempts().forEach(execution -> {
					if (execution != null) {
						subtasks.add(createSubtaskTimeInfoFromAccessExecution(execution, now));
					}
				});
			}
		}
		return new SubtasksTimesInfo(id, name, now, subtasks);
	}

	private static SubtasksTimesInfo.SubtaskTimeInfo createSubtaskTimeInfoFromAccessExecution(AccessExecution attempt, long now) {
		long[] timestamps = attempt.getStateTimestamps();
		ExecutionState status = attempt.getState();

		long scheduledTime = timestamps[ExecutionState.SCHEDULED.ordinal()];

		long start = scheduledTime > 0 ? scheduledTime : -1;
		long end = status.isTerminal() ? timestamps[status.ordinal()] : now;
		long duration = start >= 0 ? end - start : -1L;

		TaskManagerLocation location = attempt.getAssignedResourceLocation();
		String locationString = location == null ? "(unassigned)" : location.getHostname();

		Map<ExecutionState, Long> timestampMap = new HashMap<>(ExecutionState.values().length);
		for (ExecutionState state : ExecutionState.values()) {
			timestampMap.put(state, timestamps[state.ordinal()]);
		}
		return new SubtasksTimesInfo.SubtaskTimeInfo(attempt.getParallelSubtaskIndex(), locationString, duration, timestampMap, attempt.getAttemptNumber());
	}
}
