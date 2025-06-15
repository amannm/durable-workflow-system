package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.AsyncApiArguments;
import io.serverlessworkflow.api.types.AsyncApiOutboundMessage;
import io.serverlessworkflow.api.types.AsyncApiServerVariables;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import io.serverlessworkflow.api.types.ExternalResource;
import io.serverlessworkflow.api.types.EndpointConfiguration;
import io.serverlessworkflow.api.types.EndpointUri;
import io.serverlessworkflow.api.types.UriTemplate;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncApiCallHandler implements CallHandler<CallAsyncAPI> {
    private static final Logger log = LoggerFactory.getLogger(AsyncApiCallHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final StateKey<Object> resultKey;

    public AsyncApiCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    @Override
    public void handle(WorkflowContext ctx, CallAsyncAPI call) {
        AsyncApiArguments args = call.getWith();
        if (args == null || args.getDocument() == null) {
            return;
        }
        try {
            String docUrl = render(args.getDocument(), ctx);
            Map<String, Object> spec =
                    MAPPER.readValue(new java.net.URL(docUrl), new TypeReference<>() {});
            String serverUrl = selectServerUrl(spec, args);
            if (serverUrl == null) {
                log.warn("AsyncAPI server not found: {}", call);
                return;
            }
            String channel = args.getChannel();
            if (channel == null && args.getOperation() != null) {
                channel = findChannelByOperation(spec, args.getOperation());
            }
            if (channel == null) {
                log.warn("AsyncAPI channel not found: {}", call);
                return;
            }

            if (args.getMessage() != null) {
                publish(serverUrl, channel, args.getMessage(), ctx);
            } else if (args.getSubscription() != null) {
                log.info("AsyncAPI subscriptions not implemented: {}", call);
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
    private static String selectServerUrl(Map<String, Object> spec, AsyncApiArguments args) {
        Object serversObj = spec.get("servers");
        if (!(serversObj instanceof Map<?, ?> servers) || servers.isEmpty()) {
            return null;
        }
        if (args.getServer() != null && args.getServer().getName() != null) {
            Object s = servers.get(args.getServer().getName());
            if (s instanceof Map<?, ?> server) {
                return applyVariables((String) server.get("url"), args.getServer().getVariables());
            }
        }
        if (args.getProtocol() != null) {
            for (Object v : servers.values()) {
                if (v instanceof Map<?, ?> server
                        && args.getProtocol().equals(server.get("protocol"))) {
                    return applyVariables(
                            (String) server.get("url"),
                            args.getServer() != null ? args.getServer().getVariables() : null);
                }
            }
        }
        Object v = servers.values().iterator().next();
        if (v instanceof Map<?, ?> server) {
            return applyVariables(
                    (String) server.get("url"),
                    args.getServer() != null ? args.getServer().getVariables() : null);
        }
        return null;
    }

    private static String applyVariables(String url, AsyncApiServerVariables vars) {
        if (url == null) {
            return null;
        }
        if (vars == null || vars.getAdditionalProperties() == null) {
            return url;
        }
        for (var e : vars.getAdditionalProperties().entrySet()) {
            url = url.replace("{" + e.getKey() + "}", String.valueOf(e.getValue()));
        }
        return url;
    }

    @SuppressWarnings("unchecked")
    private static String findChannelByOperation(Map<String, Object> spec, String op) {
        Object channelsObj = spec.get("channels");
        if (!(channelsObj instanceof Map<?, ?> channels)) {
            return null;
        }
        for (var entry : ((Map<String, Object>) channels).entrySet()) {
            Object item = entry.getValue();
            if (item instanceof Map<?, ?> itemMap) {
                Object operations = itemMap.get("operations");
                if (operations instanceof Map<?, ?> ops && ops.containsKey(op)) {
                    return entry.getKey();
                }
                Object publish = itemMap.get("publish");
                if (publish instanceof Map<?, ?> pub
                        && op.equals(pub.get("operationId"))) {
                    return entry.getKey();
                }
                Object subscribe = itemMap.get("subscribe");
                if (subscribe instanceof Map<?, ?> sub
                        && op.equals(sub.get("operationId"))) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }

    private static void publish(
            String serverUrl,
            String channel,
            AsyncApiOutboundMessage msg,
            WorkflowContext ctx)
            throws Exception {
        String url = serverUrl.endsWith("/")
                ? serverUrl.substring(0, serverUrl.length() - 1)
                : serverUrl;
        url += channel.startsWith("/") ? channel : "/" + channel;

        String body = "";
        if (msg.getPayload() != null) {
            body = MAPPER.writeValueAsString(msg.getPayload().getAdditionalProperties());
            body = ExpressionResolver.resolveExpressions(ctx, body).orElse(body);
        }

        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(BodyPublishers.ofString(body));
        if (msg.getHeaders() != null) {
            for (var e : msg.getHeaders().getAdditionalProperties().entrySet()) {
                String value = e.getValue() == null ? "" : e.getValue().toString();
                value = ExpressionResolver.resolveExpressions(ctx, value).orElse(value);
                builder.header(e.getKey(), value);
            }
        }
        builder.header("Content-Type", "application/json");
        HttpClient.newHttpClient().send(builder.build(), BodyHandlers.discarding());
    }
}
