package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.Services;
import com.amannmalik.workflow.runtime.WorkflowRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
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
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
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

@dev.restate.sdk.annotation.Service
public class WorkflowTaskService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, Task task) {
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
            case CallHTTP t -> doHttpCall(ctx, t);
            case CallGRPC t -> log.info("Call gRPC not implemented: {}", t);
            case CallOpenAPI t -> log.info("Call OpenAPI not implemented: {}", t);
            default -> throw new UnsupportedOperationException();
        }
    }

    private static String resolveExpressions(WorkflowContext ctx, String value) {
        if (value == null) {
            return null;
        }

        value = substitute(ctx, value, Pattern.compile("\\$\\{([^}]+)}"));
        value = substitute(ctx, value, Pattern.compile("\\{([^}]+)}"));
        return value;
    }

    private static String substitute(WorkflowContext ctx, String value, Pattern pattern) {
        Matcher m = pattern.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String expr = m.group(1).trim();
            String replacement = evaluateJq(ctx, expr);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String evaluateJq(WorkflowContext ctx, String expr) {
        expr = expr.trim();
        if (!expr.startsWith(".")) {
            expr = "." + expr;
        }
        ObjectNode root = MAPPER.createObjectNode();
        java.util.Set<String> vars = new java.util.HashSet<>();
        java.util.regex.Matcher varMatcher = Pattern.compile("\\.([a-zA-Z0-9_]+)").matcher(expr);
        while (varMatcher.find()) {
            vars.add(varMatcher.group(1));
        }
        for (String key : vars) {
            ctx.get(StateKey.of(key, Object.class)).ifPresent(v -> root.set(key, MAPPER.valueToTree(v)));
        }
        try {
            JsonQuery q = JsonQuery.compile(expr, Versions.JQ_1_6);
            java.util.List<JsonNode> out = new java.util.ArrayList<>();
            q.apply(Scope.newEmptyScope(), root, out::add);
            if (out.isEmpty()) {
                return "";
            }
            JsonNode r = out.get(out.size() - 1);
            return r.isTextual() ? r.asText() : r.toString();
        } catch (Exception e) {
            log.error("Failed to evaluate expression: {}", expr, e);
            return "";
        }
    }

    private static void doHttpCall(WorkflowContext ctx, CallHTTP t) {
        HTTPArguments with = t.getWith();
        if (with == null || with.getEndpoint() == null) {
            return;
        }
        Object ep = with.getEndpoint().get();
        String epStr = ep instanceof URI u ? u.toString() : ep.toString();
        epStr = resolveExpressions(ctx, epStr);
        URI uri = URI.create(epStr);

        // apply query parameters
        if (with.getQuery() != null && with.getQuery().getHTTPQuery() != null) {
            var props = with.getQuery().getHTTPQuery().getAdditionalProperties();
            if (!props.isEmpty()) {
                var sb = new StringBuilder(uri.toString());
                sb.append(uri.getQuery() == null ? "?" : "&");
                props.forEach((k, v) -> sb.append(k).append("=").append(resolveExpressions(ctx, v)).append("&"));
                sb.setLength(sb.length() - 1);
                uri = URI.create(sb.toString());
            }
        }

        String method = with.getMethod() == null ? "GET" : with.getMethod().toUpperCase();
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri);

        // headers
        if (with.getHeaders() != null && with.getHeaders().getHTTPHeaders() != null) {
            with.getHeaders().getHTTPHeaders().getAdditionalProperties().forEach((k, v) -> builder.header(k, resolveExpressions(ctx, v)));
        }

        Object body = with.getBody();
        if (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")) {
            if (body instanceof String s) {
                builder.method(method, HttpRequest.BodyPublishers.ofString(resolveExpressions(ctx, s)));
            } else {
                builder.method(method, HttpRequest.BodyPublishers.noBody());
            }
        } else {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        }

        try {
            HttpClient client = with.isRedirect() ? HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build() : HttpClient.newHttpClient();
            client.send(builder.build(), HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void handleForkTask(WorkflowContext ctx, ForkTask task) {
        Services.callService(ctx, "ForkTaskService", "execute", task, Void.class).await();
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
                            r.getShell().getArguments().getAdditionalProperties().values().forEach(v -> cmd.add(v.toString()));
                        }
                        ProcessBuilder pb = new ProcessBuilder(cmd);
                        if (r.getShell().getEnvironment() != null) {
                            r.getShell().getEnvironment().getAdditionalProperties().forEach((k, v) -> pb.environment().put(k, v.toString()));
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

        Services.callService(ctx, "WorkflowTaskService", "execute", task, Void.class).await();
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
            resolvedDuration = resolvedDuration.plusDays(duri.getDays()).plusHours(duri.getHours()).plusMinutes(duri.getMinutes()).plusSeconds(duri.getSeconds()).plusMillis(duri.getMilliseconds());
        }
        ctx.sleep(resolvedDuration);
    }
}
