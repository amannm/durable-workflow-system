package com.amannmalik.workflow.runtime;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@dev.restate.sdk.annotation.Workflow
public class Entrypoint {

    private static final Logger log = LoggerFactory.getLogger(Entrypoint.class);

    @dev.restate.sdk.annotation.Workflow
    public void run(WorkflowContext ctx, Workflow input) {
        var taskItems = input.getDo();
        for (var taskItem : taskItems) {
            Task task = taskItem.getTask();
            TaskExecutor.execute(ctx, task);
        }
    }

    public static void main(String[] args) {
        RestateHttpServer.listen(Endpoint.bind(new Entrypoint()));
    }
}
