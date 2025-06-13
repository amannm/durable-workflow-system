package com.amannmalik.workflow.runtime;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@dev.restate.sdk.annotation.Workflow
public class Entrypoint {

    private static final Logger log = LoggerFactory.getLogger(Entrypoint.class);

    /**
     * Entry point for workflow definitions. If the workflow defines a cron
     * schedule, this method creates a {@link CronJob} to execute the
     * workflow according to that schedule. Otherwise the workflow tasks are
     * executed immediately.
     */
    @dev.restate.sdk.annotation.Workflow
    public void run(WorkflowContext ctx, Workflow input) {
        var schedule = input.getSchedule();
        if (schedule != null && schedule.getCron() != null) {
            JobRequest request = new JobRequest(
                    schedule.getCron(),
                    "Entrypoint",
                    "runInternal",
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(input));

            dev.restate.common.Target target =
                    dev.restate.common.Target.service("CronJobInitiator", "create");
            ctx.call(dev.restate.common.Request.of(
                            target,
                            dev.restate.serde.TypeTag.of(JobRequest.class),
                            dev.restate.serde.TypeTag.of(String.class),
                            request))
                    .await();
            return;
        }

        runInternal(ctx, input);
    }

    /** Executes the workflow tasks without considering scheduling. */
    @dev.restate.sdk.annotation.Workflow
    public void runInternal(WorkflowContext ctx, Workflow input) {
        var taskItems = input.getDo();
        for (var taskItem : taskItems) {
            Task task = taskItem.getTask();
            TaskExecutor.execute(ctx, task);
        }
    }


    public static void main(String[] args) {
        var builder = Endpoint.builder()
                .bind(new Entrypoint())
                .bind(new CronJobInitiator())
                .bind(new CronJob());
        RestateHttpServer.listen(builder);
    }
}
