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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                        if (with != null && with.getEndpoint() != null) {
                            Object ep = with.getEndpoint().get();
                            URI uri;
                            if (ep instanceof URI u) {
                                uri = u;
                            } else {
                                uri = URI.create(ep.toString());
                            }
                            String method = with.getMethod() == null ? "GET" : with.getMethod().toUpperCase();
                            HttpRequest.Builder builder = HttpRequest.newBuilder(uri);
                            Object body = with.getBody();
                            if (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")) {
                                if (body instanceof String s) {
                                    builder.method(method, HttpRequest.BodyPublishers.ofString(s));
                                } else {
                                    builder.method(method, HttpRequest.BodyPublishers.noBody());
                                }
                            } else {
                                builder.method(method, HttpRequest.BodyPublishers.noBody());
                            }
                            try {
                                HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.discarding());
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        }
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
                        if (r.getShell() != null && r.getShell().getCommand() != null) {
                            try {
                                new ProcessBuilder(r.getShell().getCommand()).start().waitFor();
                            } catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        }
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
                Pattern p = Pattern.compile("\\.(\\w+)\\s*==\\s*\"([^\"]*)\"");
                for (SwitchItem aSwitch : x.getSwitch()) {
                    SwitchCase switchCase = aSwitch.getSwitchCase();
                    String when = switchCase.getWhen();
                    boolean match = false;
                    if (when != null) {
                        Matcher m = p.matcher(when);
                        if (m.matches()) {
                            var key = m.group(1);
                            var expected = m.group(2);
                            var sk = StateKey.of(key, String.class);
                            match = ctx.get(sk).map(expected::equals).orElse(false);
                        }
                    }
                    if (match) {
                        FlowDirective then = switchCase.getThen();
                        if (then.getFlowDirectiveEnum() != null) {
                            return;
                        }
                    }
                }

            }
            case TryTask x -> {
                List<TaskItem> aTry = x.getTry();
                TryTaskCatch aCatch = x.getCatch();
                try {
                    for (var ti : aTry) {
                        handleTask(ctx, ti.getTask());
                    }
                } catch (Exception e) {
                    if (aCatch != null) {
                        for (var ti : aCatch.getDo()) {
                            handleTask(ctx, ti.getTask());
                        }
                    } else {
                        throw e;
                    }
                }
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
                ctx.sleep(resolvedDuration);
            }
            default -> throw new UnsupportedOperationException("Unexpected value: " + task.get());
        }
    }

    public static void main(String[] args) {
        RestateHttpServer.listen(Endpoint.bind(new Entrypoint()));
    }
}
