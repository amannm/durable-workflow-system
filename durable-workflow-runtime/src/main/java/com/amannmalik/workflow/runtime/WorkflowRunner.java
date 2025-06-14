package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.cron.CronJob;
import com.amannmalik.workflow.runtime.cron.CronJobInitiator;
import com.amannmalik.workflow.runtime.cron.CronJobRequest;
import com.amannmalik.workflow.runtime.event.EventBus;
import com.amannmalik.workflow.runtime.task.ListenTaskService;
import com.amannmalik.workflow.runtime.task.SwitchTaskService;
import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import dev.restate.serde.Serde;
import dev.restate.serde.SerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class WorkflowRunner {


    public static final ServiceDefinition DEFINITION = ServiceDefinition.of("WorkflowRunner", ServiceType.WORKFLOW, List.of(
            HandlerDefinition.of("run", HandlerType.WORKFLOW, JacksonSerdes.of(Workflow.class), Serde.VOID, HandlerRunner.of(WorkflowRunner::run, SerdeFactory.NOOP, HandlerRunner.Options.DEFAULT)),
            HandlerDefinition.of("runInternal", HandlerType.WORKFLOW, JacksonSerdes.of(Workflow.class), Serde.VOID, HandlerRunner.of(WorkflowRunner::runInternal, SerdeFactory.NOOP, HandlerRunner.Options.DEFAULT))
    ));

    private static final Logger log = LoggerFactory.getLogger(WorkflowRunner.class);

    public static void run(WorkflowContext ctx, Workflow input) {
        var schedule = input.getSchedule();
        if (schedule != null && schedule.getCron() != null) {
            CronJobRequest request = new CronJobRequest(schedule.getCron(), "WorkflowRunner", "runInternal", Optional.empty(), Optional.empty(), Optional.of(input));
            Services.callService(ctx, "CronJobInitiator", "create", request, String.class).await();
        } else {
            runInternal(ctx, input);
        }
    }

    public static void runInternal(WorkflowContext ctx, Workflow input) {
        var taskItems = input.getDo();
        if (taskItems == null || taskItems.isEmpty()) {
            return;
        }

        java.util.Map<String, Integer> index = new java.util.HashMap<>();
        for (int i = 0; i < taskItems.size(); i++) {
            index.put(taskItems.get(i).getName(), i);
        }

        WorkflowTaskService wts = new WorkflowTaskService();
        int i = 0;
        while (i < taskItems.size()) {
            var ti = taskItems.get(i);
            wts.execute(ctx, ti.getTask());

            String nextName = ctx.get(SwitchTaskService.NEXT).orElse(null);
            if (nextName != null) {
                ctx.clear(SwitchTaskService.NEXT);
                if ("EXIT".equals(nextName) || "END".equals(nextName)) {
                    break;
                }
                Integer ni = index.get(nextName);
                if (ni != null) {
                    i = ni;
                    continue;
                }
            }

            var base = (io.serverlessworkflow.api.types.TaskBase) ti.getTask().get();
            var then = base.getThen();
            if (then != null) {
                if (then.getFlowDirectiveEnum() != null) {
                    var fd = then.getFlowDirectiveEnum();
                    if (fd == io.serverlessworkflow.api.types.FlowDirectiveEnum.EXIT || fd == io.serverlessworkflow.api.types.FlowDirectiveEnum.END) {
                        break;
                    }
                } else if (then.getString() != null) {
                    Integer ni = index.get(then.getString());
                    if (ni != null) {
                        i = ni;
                        continue;
                    }
                }
            }
            i++;
        }
    }

    public static void main(String[] args) {
        var builder = Endpoint.builder()
                .bind(WorkflowRunner.DEFINITION)
                .bind(WorkflowTaskService.DEFINITION)
                .bind(ListenTaskService.DEFINITION)
                .bind(EventBus.DEFINITION)
                .bind(CronJobInitiator.DEFINITION)
                .bind(CronJob.DEFINITION);
        RestateHttpServer.listen(builder);
    }
}
