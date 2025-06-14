package com.amannmalik.workflow.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.*;
import com.amannmalik.workflow.runtime.WorkflowRegistry;
import com.amannmalik.workflow.runtime.Entrypoint;
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

/**
 * Executes individual workflow tasks.
 */
final class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    private TaskExecutor() {
    }

    static void execute(WorkflowContext ctx, Task task) {
        switch (task.get()) {
            case CallTask x -> handleCallTask(ctx, x);
            case DoTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ForkTask x -> handleForkTask(ctx, x);
            case EmitTask x -> handleEmitTask(ctx, x);
            case ForTask x -> x.getDo().forEach(t -> execute(ctx, t.getTask()));
            case ListenTask x -> {
                log.info("Listen task not implemented: {}", x);
            }
            case RaiseTask x -> {
                try {
                    log.warn("Raise event: {}", new ObjectMapper().writeValueAsString(x.getRaise()));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException(e);
                }
            }
            case RunTask x -> handleRunTask(ctx, x);
            case SetTask x -> handleSetTask(ctx, x);
            case SwitchTask x -> handleSwitchTask(ctx, x);
            case TryTask x -> handleTryTask(ctx, x);
            case WaitTask x -> handleWaitTask(ctx, x);
            default -> throw new UnsupportedOperationException("Unexpected task: " + task.get());
        }
    }

    private static void handleCallTask(WorkflowContext ctx, CallTask x) {
        switch (x.get()) {
            case CallFunction t -> {
                log.info("Call function not implemented: {}", t);
            }
            case CallAsyncAPI t -> {
                log.info("Call AsyncAPI not implemented: {}", t);
            }
            case CallHTTP t -> doHttpCall(t);
            case CallGRPC t -> log.info("Call gRPC not implemented: {}", t);
            case CallOpenAPI t -> log.info("Call OpenAPI not implemented: {}", t);
            default -> throw new UnsupportedOperationException();
        }
    }

    private static void doHttpCall(CallHTTP t) {
        HTTPArguments with = t.getWith();
        if (with == null || with.getEndpoint() == null) {
            return;
        }
        Object ep = with.getEndpoint().get();
        URI uri = ep instanceof URI u ? u : URI.create(ep.toString());

        // apply query parameters
        if (with.getQuery() != null && with.getQuery().getHTTPQuery() != null) {
            var props = with.getQuery().getHTTPQuery().getAdditionalProperties();
            if (!props.isEmpty()) {
                var sb = new StringBuilder(uri.toString());
                sb.append(uri.getQuery() == null ? "?" : "&");
                props.forEach((k, v) -> sb.append(k).append("=").append(v).append("&"));
                sb.setLength(sb.length() - 1);
                uri = URI.create(sb.toString());
            }
        }

        String method = with.getMethod() == null ? "GET" : with.getMethod().toUpperCase();
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri);

        // headers
        if (with.getHeaders() != null && with.getHeaders().getHTTPHeaders() != null) {
            with.getHeaders().getHTTPHeaders().getAdditionalProperties()
                    .forEach(builder::header);
        }

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

    private static void handleForkTask(WorkflowContext ctx, ForkTask x) {
        ForkTaskConfiguration fork = x.getFork();
        List<TaskItem> branches = fork.getBranches();
        List<dev.restate.sdk.DurableFuture<Void>> futures = new java.util.ArrayList<>();
        int i = 0;
        for (var branch : branches) {
            final int branchId = i++;
            futures.add(ctx.runAsync("branch-" + branchId, TypeTag.of(Void.class),
                    dev.restate.sdk.common.RetryPolicy.defaultPolicy(), () -> {
                        execute(ctx, branch.getTask());
                        return null;
                    }));
        }
        for (var f : futures) {
            try {
                f.await();
            } catch (dev.restate.sdk.common.TerminalException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static void handleEmitTask(WorkflowContext ctx, EmitTask x) {
        EmitTaskConfiguration emit = x.getEmit();
        EmitEventDefinition event = emit.getEvent();
        EventProperties with = event.getWith();
        EventData data = with.getData();
        Target target = Target.service("MyService", "myHandler");
        Object object = data.getObject();
        if (object instanceof String sv) {
            ctx.send(Request.of(target, TypeTag.of(String.class), TypeTag.of(String.class), sv));
        } else if (object instanceof Integer iv) {
            ctx.send(Request.of(target, TypeTag.of(Integer.class), TypeTag.of(Integer.class), iv));
        } else if (object instanceof Long lv) {
            ctx.send(Request.of(target, TypeTag.of(Long.class), TypeTag.of(Long.class), lv));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private static void handleRunTask(WorkflowContext ctx, RunTask task) {
        RunTaskConfigurationUnion run = task.getRun();
        switch (run.get()) {
            case RunContainer r -> log.info("Run container not implemented: {}", r);
            case RunScript r -> log.info("Run script not implemented: {}", r);
            case RunShell r -> {
                if (r.getShell() != null && r.getShell().getCommand() != null) {
                    try {
                        List<String> cmd = new java.util.ArrayList<>();
                        cmd.add(r.getShell().getCommand());
                        if (r.getShell().getArguments() != null) {
                            r.getShell().getArguments().getAdditionalProperties().values()
                                    .forEach(v -> cmd.add(v.toString()));
                        }
                        ProcessBuilder pb = new ProcessBuilder(cmd);
                        if (r.getShell().getEnvironment() != null) {
                            r.getShell().getEnvironment().getAdditionalProperties()
                                    .forEach((k, v) -> pb.environment().put(k, v.toString()));
                        }
                        pb.start().waitFor();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            }
            case RunWorkflow r -> handleRunWorkflow(ctx, r);
            default -> throw new UnsupportedOperationException();
        }
    }

    private static void handleRunWorkflow(WorkflowContext ctx, RunWorkflow run) {
        var cfg = run.getWorkflow();
        if (cfg == null) {
            return;
        }
        Workflow wf = WorkflowRegistry.get(cfg.getNamespace(), cfg.getName(), cfg.getVersion());
        if (wf == null) {
            log.warn("Sub-workflow not found: {}:{}:{}", cfg.getNamespace(), cfg.getName(), cfg.getVersion());
            return;
        }
        new Entrypoint().runInternal(ctx, wf);
    }

    private static void handleSetTask(WorkflowContext ctx, SetTask x) {
        var s = x.getSet();
        var k = s.getString();
        var v = s.get();
        switch (v) {
            case String sv -> ctx.set(StateKey.of(k, String.class), sv);
            case Long sl -> ctx.set(StateKey.of(k, Long.class), sl);
            case Integer si -> ctx.set(StateKey.of(k, Integer.class), si);
            case Double sd -> ctx.set(StateKey.of(k, Double.class), sd);
            case Boolean sb -> ctx.set(StateKey.of(k, Boolean.class), sb);
            default -> throw new UnsupportedOperationException();
        }
    }

    private static void handleSwitchTask(WorkflowContext ctx, SwitchTask x) {
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

    private static void handleTryTask(WorkflowContext ctx, TryTask x) {
        List<TaskItem> aTry = x.getTry();
        TryTaskCatch aCatch = x.getCatch();
        try {
            for (var ti : aTry) {
                execute(ctx, ti.getTask());
            }
        } catch (Exception e) {
            if (aCatch != null) {
                for (var ti : aCatch.getDo()) {
                    execute(ctx, ti.getTask());
                }
            } else {
                throw e;
            }
        }
    }

    private static void handleWaitTask(WorkflowContext ctx, WaitTask x) {
        var wtc = x.getWait();
        var de = wtc.getDurationExpression();
        Duration resolvedDuration = Duration.ZERO;
        if (de != null) {
            resolvedDuration = Duration.parse(de);
        } else {
            var duri = wtc.getDurationInline();
            resolvedDuration = resolvedDuration.plusDays(duri.getDays()).plusHours(duri.getHours())
                    .plusMinutes(duri.getMinutes()).plusSeconds(duri.getSeconds()).plusMillis(duri.getMilliseconds());
        }
        ctx.sleep(resolvedDuration);
    }
}
