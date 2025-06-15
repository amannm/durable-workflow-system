package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.EndpointConfiguration;
import io.serverlessworkflow.api.types.EndpointUri;
import io.serverlessworkflow.api.types.ExternalResource;
import io.serverlessworkflow.api.types.UriTemplate;
import io.serverlessworkflow.api.types.OpenAPIArguments;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenApiCallHandler implements CallHandler<CallOpenAPI> {
    private static final Logger log = LoggerFactory.getLogger(OpenApiCallHandler.class);
    private final StateKey<Object> resultKey;
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    public OpenApiCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    @Override
    public void handle(WorkflowContext ctx, CallOpenAPI call) {
        OpenAPIArguments args = call.getWith();
        if (args == null || args.getDocument() == null || args.getOperationId() == null) {
            return;
        }
        try {
            String docUrl = render(args.getDocument(), ctx);
            Map<String, Object> spec =
                    MAPPER.readValue(new java.net.URL(docUrl), new TypeReference<>() {});

            Operation op = findOperation(spec, args.getOperationId());
            if (op == null) {
                log.warn("OpenAPI operation not found: {}", args.getOperationId());
                return;
            }

            String baseUrl = selectServerUrl(spec);
            if (baseUrl == null) {
                baseUrl = "";
            }
            String url = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
            url += op.path.startsWith("/") ? op.path : "/" + op.path;

            Map<String, Object> params = new HashMap<>();
            if (args.getParameters() != null) {
                params.putAll(args.getParameters().getAdditionalProperties());
            }

            for (var e : params.entrySet()) {
                String val = String.valueOf(e.getValue());
                val = ExpressionResolver.resolveExpressions(ctx, val).orElse(val);
                String encVal = URLEncoder.encode(val, StandardCharsets.UTF_8);
                if (url.contains("{" + e.getKey() + "}")) {
                    url = url.replace("{" + e.getKey() + "}", encVal);
                }
            }

            StringBuilder qs = new StringBuilder();
            for (var e : params.entrySet()) {
                if (op.path.contains("{" + e.getKey() + "}")) {
                    continue;
                }
                String val = String.valueOf(e.getValue());
                val = ExpressionResolver.resolveExpressions(ctx, val).orElse(val);
                String encVal = URLEncoder.encode(val, StandardCharsets.UTF_8);
                if (qs.length() == 0 && !url.contains("?")) {
                    qs.append('?');
                } else {
                    qs.append('&');
                }
                qs.append(e.getKey()).append('=').append(encVal);
            }
            url += qs.toString();

            HttpClient client =
                    args.isRedirect()
                            ? HttpClient.newBuilder().followRedirects(Redirect.ALWAYS).build()
                            : HttpClient.newHttpClient();
            HttpRequest request =
                    HttpRequest.newBuilder(URI.create(url))
                            .method(op.method, BodyPublishers.noBody())
                            .build();
            HttpResponse<String> resp = client.send(request, BodyHandlers.ofString());

            OpenAPIArguments.WithOpenAPIOutput out =
                    args.getOutput() == null
                            ? OpenAPIArguments.WithOpenAPIOutput.CONTENT
                            : args.getOutput();
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

    private static String render(ExternalResource doc, WorkflowContext ctx) {
        Object ep = doc.getEndpoint().get();
        String str;
        if (ep instanceof URI u) {
            str = u.toString();
        } else if (ep instanceof EndpointConfiguration cfg) {
            Object uo = cfg.getUri().get();
            if (uo instanceof UriTemplate ut && ut.getLiteralUri() != null) {
                str = ut.getLiteralUri().toString();
            } else {
                str = uo.toString();
            }
        } else if (ep instanceof EndpointUri euri) {
            Object uo = euri.get();
            if (uo instanceof UriTemplate ut && ut.getLiteralUri() != null) {
                str = ut.getLiteralUri().toString();
            } else {
                str = uo.toString();
            }
        } else if (ep instanceof UriTemplate ut && ut.getLiteralUri() != null) {
            str = ut.getLiteralUri().toString();
        } else {
            str = ep.toString();
        }
        return ExpressionResolver.resolveExpressions(ctx, str).orElse(str);
    }

    @SuppressWarnings("unchecked")
    private static Operation findOperation(Map<String, Object> spec, String id) {
        Object pathsObj = spec.get("paths");
        if (!(pathsObj instanceof Map<?, ?> paths)) {
            return null;
        }
        for (var entry : ((Map<String, Object>) paths).entrySet()) {
            String path = entry.getKey();
            Object opsObj = entry.getValue();
            if (opsObj instanceof Map<?, ?> ops) {
                for (var me : ((Map<String, Object>) ops).entrySet()) {
                    String m = me.getKey();
                    Object opObj = me.getValue();
                    if (opObj instanceof Map<?, ?> op && id.equals(op.get("operationId"))) {
                        return new Operation(path, m.toUpperCase());
                    }
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String selectServerUrl(Map<String, Object> spec) {
        Object servers = spec.get("servers");
        if (servers instanceof List<?> list && !list.isEmpty()) {
            Object s = list.getFirst();
            if (s instanceof Map<?, ?> map && map.get("url") != null) {
                return map.get("url").toString();
            }
        }
        if (spec.get("host") != null) {
            String scheme = "http";
            Object schemes = spec.get("schemes");
            if (schemes instanceof List<?> list && !list.isEmpty()) {
                scheme = list.getFirst().toString();
            }
            String basePath = spec.get("basePath") == null ? "" : spec.get("basePath").toString();
            return scheme + "://" + spec.get("host") + basePath;
        }
        return null;
    }

    private record Operation(String path, String method) {}
}
