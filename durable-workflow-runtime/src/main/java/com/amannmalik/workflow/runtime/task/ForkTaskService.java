package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.TaskItem;

import java.util.ArrayList;
import java.util.List;

public class ForkTaskService {

  public static final ServiceDefinition DEFINITION =
          DefinitionHelper.taskService(ForkTaskService.class, ForkTask.class, ForkTaskService::execute);

  public static void execute(WorkflowContext ctx, ForkTask task) {
    var fork = task.getFork();
    int i = 0;
    List<DurableFuture<Void>> futures = new ArrayList<>();
    for (TaskItem item : fork.getBranches()) {
      String name = "branch-" + (i++);
      futures.add(
              ctx.runAsync(
                      name,
                      TypeTag.of(Void.class),
                      RetryPolicy.defaultPolicy(),
                      () -> {
                        Services.callService(
                                        ctx, "WorkflowTaskService", "execute", item.getTask(), Void.class)
                                .await();
                        return null;
                      }));
    }
    for (DurableFuture<Void> f : futures) {
      f.await();
    }
  }
}
