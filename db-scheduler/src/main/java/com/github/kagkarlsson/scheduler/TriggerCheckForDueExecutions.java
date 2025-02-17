/**
 * Copyright (C) Gustav Karlsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

class TriggerCheckForDueExecutions implements SchedulerClientEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(TriggerCheckForDueExecutions.class);
    private SchedulerState schedulerState;
    private Clock clock;
    private Waiter executeDueWaiter;

    public TriggerCheckForDueExecutions(SchedulerState schedulerState, Clock clock, Waiter executeDueWaiter) {
        this.schedulerState = schedulerState;
        this.clock = clock;
        this.executeDueWaiter = executeDueWaiter;
    }

    @Override
    public void newEvent(ClientEvent event) {
        ClientEvent.ClientEventContext ctx = event.getContext();
        ClientEvent.EventType eventType = ctx.getEventType();

        if (!schedulerState.isStarted() || schedulerState.isShuttingDown()) {
            LOG.debug("Will not act on scheduling event for execution (task: '{}', id: '{}') as scheduler is starting or shutting down.",
                    ctx.getTaskInstanceId().getTaskName(), ctx.getTaskInstanceId().getId());
            return;
        }

        if (eventType == ClientEvent.EventType.SCHEDULE || eventType == ClientEvent.EventType.RESCHEDULE) {

            Instant scheduledToExecutionTime = ctx.getExecutionTime();
            if (scheduledToExecutionTime.toEpochMilli() <= clock.now().toEpochMilli()) {
                LOG.debug("Task-instance scheduled to run directly, triggering check for due executions (unless it is already running). Task: {}, instance: {}",
                        ctx.getTaskInstanceId().getTaskName(), ctx.getTaskInstanceId().getId());
                executeDueWaiter.wakeOrSkipNextWait();
            }
        }
    }
}
