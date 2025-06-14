package com.amannmalik.workflow.runtime.task.call;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import io.serverlessworkflow.api.types.CallFunction;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.HTTPArguments;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@dev.restate.sdk.annotation.Service
public class CallTaskService {

    private static final Logger log = LoggerFactory.getLogger(CallTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @dev.restate.sdk.annotation.Handler
    public void execute(WorkflowContext ctx, CallTask task) {
        switch (task.get()) {
            case CallFunction t -> log.info("Call function not implemented: {}", t);
            case CallAsyncAPI t -> log.info("Call AsyncAPI not implemented: {}", t);
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
            List<JsonNode> out = new java.util.ArrayList<>();
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
}
