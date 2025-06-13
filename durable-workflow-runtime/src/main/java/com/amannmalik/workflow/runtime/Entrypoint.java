package com.amannmalik.workflow.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import io.serverlessworkflow.api.types.CallFunction;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.DoTask;
import io.serverlessworkflow.api.types.EmitEventDefinition;
import io.serverlessworkflow.api.types.EmitTask;
import io.serverlessworkflow.api.types.EmitTaskConfiguration;
import io.serverlessworkflow.api.types.EventData;
import io.serverlessworkflow.api.types.EventProperties;
import io.serverlessworkflow.api.types.FlowDirective;
import io.serverlessworkflow.api.types.ForTask;
import io.serverlessworkflow.api.types.ForkTask;
import io.serverlessworkflow.api.types.ForkTaskConfiguration;
import io.serverlessworkflow.api.types.HTTPArguments;
import io.serverlessworkflow.api.types.ListenTask;
import io.serverlessworkflow.api.types.RaiseTask;
import io.serverlessworkflow.api.types.RunContainer;
import io.serverlessworkflow.api.types.RunScript;
import io.serverlessworkflow.api.types.RunShell;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.RunTaskConfigurationUnion;
import io.serverlessworkflow.api.types.RunWorkflow;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SwitchCase;
import io.serverlessworkflow.api.types.SwitchItem;
import io.serverlessworkflow.api.types.SwitchTask;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.TryTaskCatch;
import io.serverlessworkflow.api.types.WaitTask;
import io.serverlessworkflow.api.types.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

@dev.restate.sdk.annotation.Workflow
public class Entrypoint {

    private static final Logger log = LoggerFactory.getLogger(Entrypoint.class);

    @dev.restate.sdk.annotation.Workflow
    public void run(WorkflowContext ctx, Workflow input) {
        // map serverless workflow definition into restate workflow definition
        var taskItems = input.getDo();
        for (var taskItem : taskItems) {
            var task = taskItem.getTask();
            handleTask(ctx, task);
        }
    }

    private static void handleTask(WorkflowContext ctx, Task task) {
        switch (task.get()) {
            case CallTask x -> {
                switch (x.get()) {
                    case CallFunction t -> {
                        // ??
                    }
                    case CallAsyncAPI t -> {
                        // use async api client
                    }
                    case CallHTTP t -> {
                        HTTPArguments with = t.getWith();
                        // TODO: use java http client to call endpoint
                    }
                    case CallGRPC t -> {
                        // use grpc client
                    }
                    case CallOpenAPI t -> {
                        // use java http client?
                    }
                    default -> throw new UnsupportedOperationException();
                }
                return;
            }
            case DoTask x -> {
                for (var taskItem : x.getDo()) {
                    var task1 = taskItem.getTask();
                    handleTask(ctx, task1);


                }
            }
            case ForkTask x -> {
                ForkTaskConfiguration fork = x.getFork();
                List<TaskItem> branches = fork.getBranches();
                // TODO: parallelize and join
                for (var branch : branches) {
                    for (var taskItem : List.of(branch)) {
                        var task1 = taskItem.getTask();
                        handleTask(ctx, task1);


                    }
                }
            }
            case EmitTask x -> {
                //TODO: this
                EmitTaskConfiguration emit = x.getEmit();
                EmitEventDefinition event = emit.getEvent();
                EventProperties with = event.getWith();
                EventData data = with.getData();
                Target target = Target.service("MyService", "myHandler"); // or virtualObject or workflow
                Object object = data.getObject();
                switch (object) {
                    case String sv -> {
                        // todo:
                        ctx.send(Request.of(target, TypeTag.of(String.class), TypeTag.of(String.class), sv));
                    }
                    default -> throw new UnsupportedOperationException();
                }
            }
            case ForTask x -> {
                var fordo = x.getDo();
                for (var taskItem : fordo) {
                    var task1 = taskItem.getTask();
                    handleTask(ctx, task1);


                }
            }
            case ListenTask x -> {
                // ???
            }
            case RaiseTask x -> {
                try {
                    var dddd = new ObjectMapper().writeValueAsString(x.getRaise());
                    throw new IllegalStateException(dddd);
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            }
            case RunTask x -> {
                RunTaskConfigurationUnion run = x.getRun();
                switch (run.get()) {
                    case RunContainer r -> {
                        // todo:
                    }
                    case RunScript r -> {
                        // todo:
                    }
                    case RunShell r -> {
                        // todo:
                    }
                    case RunWorkflow r -> {
                        // todo:
                    }
                    default -> throw new UnsupportedOperationException();
                }
            }
            case SetTask x -> {
                var s = x.getSet();
                var k = s.getString();
                var v = s.get();
                switch (v) {
                    case String sv -> {
                        var sk = StateKey.of(k, String.class);
                        ctx.set(sk, sv);
                    }
                    case Long sl -> {
                        var sk = StateKey.of(k, Long.class);
                        ctx.set(sk, sl);
                    }
                    // TODO: other expected java types?
                    default -> throw new UnsupportedOperationException();
                }
                return;
            }
            case SwitchTask x -> {
                for (SwitchItem aSwitch : x.getSwitch()) {
                    SwitchCase switchCase = aSwitch.getSwitchCase();
                    String when = switchCase.getWhen();
                    // todo: evaluate when condition
                    FlowDirective then = switchCase.getThen();
                    switch (then.getFlowDirectiveEnum()) {
                        case CONTINUE -> {

                        }
                        case EXIT -> {

                        }
                        case END -> {

                        }
                    }

                }

            }
            case TryTask x -> {
                // TODO:
                List<TaskItem> aTry = x.getTry();
                TryTaskCatch aCatch = x.getCatch();

            }
            case WaitTask x -> {
                var wtc = x.getWait();
                var de = wtc.getDurationExpression();
                Duration resolvedDuration = Duration.ZERO;
                if (de != null) {
                    resolvedDuration = Duration.parse(de);
                } else {
                    var duri = wtc.getDurationInline();
                    resolvedDuration = resolvedDuration.plusDays(duri.getDays()).plusHours(duri.getHours()).plusMinutes(duri.getMinutes()).plusSeconds(duri.getSeconds()).plusMillis(duri.getMilliseconds());
                }
                try {
                    ctx.wait(resolvedDuration.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            default -> throw new UnsupportedOperationException("Unexpected value: " + task.get());
        }
    }

    public static void main(String[] args) {
        RestateHttpServer.listen(Endpoint.bind(new Entrypoint()));
    }
}
