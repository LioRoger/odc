/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.odc.service.task.supervisor.protocol;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.oceanbase.odc.common.json.JsonUtils;
import com.oceanbase.odc.service.task.caller.JobContext;
import com.oceanbase.odc.service.task.supervisor.TaskSupervisor;
import com.oceanbase.odc.service.task.supervisor.endpoint.ExecutorEndpoint;
import com.oceanbase.odc.service.task.supervisor.protocol.command.GeneralTaskCommand;
import com.oceanbase.odc.service.task.supervisor.protocol.command.StartTaskCommand;

/**
 * @author longpeng.zlp
 * @date 2024/10/29 17:45
 */
public class TaskCommandExecutor {
    public TaskSupervisor taskSupervisor;

    public void onCommand(TaskCommand taskCommand, Consumer<String> responseConsumer) {
        String ret = null;
        switch (taskCommand.commandType()) {
            case START:
                StartTaskCommand startTaskCommand = (StartTaskCommand) taskCommand;
                ExecutorEndpoint endpoint = taskSupervisor.startTask(startTaskCommand.getJobContext(), startTaskCommand.getProcessConfig());
                ret = JsonUtils.toJson(endpoint);
                break;
            default:
                boolean succeed = callTaskSupervisorFunc((GeneralTaskCommand) taskCommand);
                ret = String.valueOf(succeed);
                break;
        }
        responseConsumer.accept(ret);
    }

    protected boolean callTaskSupervisorFunc(GeneralTaskCommand generalTaskCommand) {
        switch (generalTaskCommand.commandType()) {
            case STOP:
                return taskSupervisor.stopTask(generalTaskCommand.getExecutorEndpoint(), generalTaskCommand.getJobContext());
            case MODIFY:
                return taskSupervisor.modifyTask(generalTaskCommand.getExecutorEndpoint(), generalTaskCommand.getJobContext());
            case FINISH:
                return taskSupervisor.finishTask(generalTaskCommand.getExecutorEndpoint(), generalTaskCommand.getJobContext());
            case CAN_BE_FINISHED:
                return taskSupervisor.canBeFinished(generalTaskCommand.getExecutorEndpoint(), generalTaskCommand.getJobContext());
            default:
                throw new IllegalStateException("not recognized command " + generalTaskCommand);
        }
    }
}
