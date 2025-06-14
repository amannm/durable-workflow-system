package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.HTTPArguments;
import io.serverlessworkflow.api.types.HTTPArguments.HTTPOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.HashMap;
import java.util.Map;

public class HttpCallHandler implements CallHandler<CallHTTP> {
    private static final Logger log = LoggerFactory.getLogger(HttpCallHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final StateKey<Object> resultKey;

    public HttpCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    @Override
    public void handle(WorkflowContext ctx, CallHTTP t) {
        HTTPArguments with = t.getWith();
        if (with == null || with.getEndpoint() == null) {
            return;
        }
        Object ep = with.getEndpoint().get();
        String epStr = ep instanceof URI u ? u.toString() : ep.toString();
        epStr = ExpressionResolver.resolveExpressions(ctx, epStr).orElseThrow();
        URI uri = URI.create(epStr);

        if (with.getQuery() != null && with.getQuery().getHTTPQuery() != null) {
            var props = with.getQuery().getHTTPQuery().getAdditionalProperties();
            if (!props.isEmpty()) {
                var sb = new StringBuilder(uri.toString());
                sb.append(uri.getQuery() == null ? "?" : "&");
                props.forEach(
                        (k, v) ->
                                sb.append(k)
                                        .append("=")
                                        .append(
                                                ExpressionResolver
                                                        .resolveExpressions(ctx, v)
                                                        .orElse("") )
                                        .append("&"));
                sb.setLength(sb.length() - 1);
                uri = URI.create(sb.toString());
            }
        }

        String method = with.getMethod() == null ? "GET" : with.getMethod().toUpperCase();
        Builder builder = HttpRequest.newBuilder(uri);

        if (with.getHeaders() != null && with.getHeaders().getHTTPHeaders() != null) {
            with.getHeaders()
                    .getHTTPHeaders()
                    .getAdditionalProperties()
                    .forEach(
                            (k, v) ->
                                    builder.header(
                                            k,
                                            ExpressionResolver
                                                    .resolveExpressions(ctx, v)
                                                    .orElse("")));
        }

        Object body = with.getBody();
        if (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")) {
            if (body instanceof String s) {
                builder.method(
                        method,
                        BodyPublishers.ofString(
                                ExpressionResolver
                                        .resolveExpressions(ctx, s)
                                        .orElse("")));
            } else {
                builder.method(method, BodyPublishers.noBody());
            }
        } else {
            builder.method(method, BodyPublishers.noBody());
        }

        try {
            HttpClient client =
                    with.isRedirect()
                            ? HttpClient.newBuilder().followRedirects(Redirect.ALWAYS).build()
                            : HttpClient.newHttpClient();
            HttpResponse<String> resp = client.send(builder.build(), BodyHandlers.ofString());
            HTTPOutput out = with.getOutput() == null ? HTTPOutput.CONTENT : with.getOutput();
            switch (out) {
                case RAW -> ctx.set(resultKey, resp.body());
                case CONTENT -> {
                    try {
                        Object obj = MAPPER.readValue(resp.body(), Object.class);
                        ctx.set(resultKey, obj);
                    } catch (Exception e) {
                        ctx.set(resultKey, resp.body());
                    }
                }
                case RESPONSE -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("status", resp.statusCode());
                    map.put("headers", resp.headers().map());
                    map.put("body", resp.body());
                    ctx.set(resultKey, map);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
