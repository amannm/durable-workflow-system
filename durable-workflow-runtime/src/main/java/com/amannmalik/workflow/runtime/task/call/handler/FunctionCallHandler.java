package com.amannmalik.workflow.runtime.task.call.handler;

import com.amannmalik.workflow.runtime.Services;
import com.amannmalik.workflow.runtime.task.call.CallTaskService;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallFunction;
import io.serverlessworkflow.api.types.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class FunctionCallHandler implements CallHandler<CallFunction> {
    private static final Logger log = LoggerFactory.getLogger(FunctionCallHandler.class);
    private static final ObjectMapper MAPPER;
    private final StateKey<Object> resultKey;

    public FunctionCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    static {
        MAPPER = new ObjectMapper(new YAMLFactory());
        MAPPER.registerModule(new JavaTimeModule());
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void handle(WorkflowContext ctx, CallFunction call) {
        String name = call.getCall();
        if (name == null || name.isBlank()) {
            return;
        }

        Map<String, Task> funcs = ctx.get(CallTaskService.FUNCTIONS).orElse(Map.of());
        Task fn = funcs.get(name);
        if (fn != null) {
            Map<String, Object> args = call.getWith() == null ? Map.of() : call.getWith().getAdditionalProperties();
            Map<String, Object> prev = new HashMap<>();
            for (var e : args.entrySet()) {
                StateKey<Object> k = StateKey.of(e.getKey(), Object.class);
                ctx.get(k).ifPresent(v -> prev.put(e.getKey(), v));
                ctx.set(k, e.getValue());
            }
            Services.callService(ctx, "WorkflowTaskService", "execute", fn, Void.class).await();
            for (var e : args.entrySet()) {
                StateKey<Object> k = StateKey.of(e.getKey(), Object.class);
                if (prev.containsKey(e.getKey())) {
                    ctx.set(k, prev.get(e.getKey()));
                } else {
                    ctx.clear(k);
                }
            }
            return;
        }

        try {
            String url = toRawUrl(name);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET().build();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
            Task task = MAPPER.readValue(resp.body(), Task.class);
            Services.callService(ctx, "WorkflowTaskService", "execute", task, Void.class).await();
        } catch (Exception e) {
            log.error("Failed to execute function {}", name, e);
            throw new IllegalStateException(e);
        }
    }

    private static String toRawUrl(String url) {
        if (url.startsWith("https://github.com/")) {
            url = url.replaceFirst("https://github.com/", "https://raw.githubusercontent.com/");
            url = url.replaceFirst("/tree/", "/refs/heads/");
        }
        return url;
    }
}
