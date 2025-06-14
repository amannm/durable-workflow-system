package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.cron.CronJob;
import com.amannmalik.workflow.runtime.cron.CronJobInitiator;
import com.amannmalik.workflow.runtime.cron.CronJobRequest;
import com.amannmalik.workflow.runtime.task.EmitTaskService;
import com.amannmalik.workflow.runtime.task.ForkTaskService;
import com.amannmalik.workflow.runtime.task.SetTaskService;
import com.amannmalik.workflow.runtime.task.SwitchTaskService;
import com.amannmalik.workflow.runtime.task.TryTaskService;
import com.amannmalik.workflow.runtime.task.WaitTaskService;
import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import com.amannmalik.workflow.runtime.task.call.CallTaskService;
import com.amannmalik.workflow.runtime.task.run.RunTaskService;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@dev.restate.sdk.annotation.Workflow
public class WorkflowRunner {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRunner.class);

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
            CronJobRequest request = new CronJobRequest(schedule.getCron(), "WorkflowRunner", "runInternal", Optional.empty(), Optional.empty(), Optional.of(input));
            Services.callService(ctx, "CronJobInitiator", "create", request, String.class).await();
        } else {
            runInternal(ctx, input);
        }
    }

    @dev.restate.sdk.annotation.Workflow
    public void runInternal(WorkflowContext ctx, Workflow input) {
        var taskItems = input.getDo();
        WorkflowTaskService wts = new WorkflowTaskService();
        for (var ti : taskItems) {
            wts.execute(ctx, ti.getTask());
        }
    }

    public static void main(String[] args) {
        var builder = Endpoint.builder()
                .bind(new WorkflowRunner())
                .bind(new WorkflowTaskService())
                .bind(new ForkTaskService())
                .bind(new RunTaskService())
                .bind(new CallTaskService())
                .bind(new EmitTaskService())
                .bind(new SetTaskService())
                .bind(new SwitchTaskService())
                .bind(new TryTaskService())
                .bind(new WaitTaskService())
                .bind(new CronJobInitiator())
                .bind(new CronJob());
        RestateHttpServer.listen(builder);
    }
}
