package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.Error;
import io.serverlessworkflow.api.types.RaiseTask;
import io.serverlessworkflow.api.types.RaiseTaskConfiguration;
import io.serverlessworkflow.api.types.RaiseTaskError;

/** Service executing the raise task as defined in the DSL. */
public class RaiseTaskService {

  public static final ServiceDefinition DEFINITION =
      DefinitionHelper.taskService(
          RaiseTaskService.class, RaiseTask.class, RaiseTaskService::execute);

  public static void execute(WorkflowContext ctx, RaiseTask task) {
    RaiseTaskConfiguration rc = task.getRaise();
    if (rc == null || rc.getError() == null) {
      return;
    }
    RaiseTaskError rte = rc.getError();
    Object val = rte.get();
    if (val instanceof Error err) {
      throw new WorkflowErrorException(err);
    } else {
      // References to reusable errors not yet supported
      throw new RuntimeException("Referenced errors not supported");
    }
  }
}
