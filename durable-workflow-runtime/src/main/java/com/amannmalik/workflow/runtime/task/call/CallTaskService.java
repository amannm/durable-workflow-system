package com.amannmalik.workflow.runtime.task.call;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import io.serverlessworkflow.api.types.CallFunction;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.HTTPArguments;
import io.serverlessworkflow.api.types.HTTPArguments.HTTPOutput;
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

public class CallTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            CallTaskService.class,
            CallTask.class,
            CallTaskService::execute
    );

    private static final Logger log = LoggerFactory.getLogger(CallTaskService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final StateKey<Object> RESULT = StateKey.of("call-result", Object.class);

    public static void execute(WorkflowContext ctx, CallTask task) {
        switch (task.get()) {
            case CallFunction t -> log.info("Call function not implemented: {}", t);
            case CallAsyncAPI t -> log.info("Call AsyncAPI not implemented: {}", t);
            case CallHTTP t -> doHttpCall(ctx, t);
            case CallGRPC t -> doGrpcCall(ctx, t);
            case CallOpenAPI t -> log.info("Call OpenAPI not implemented: {}", t);
            default -> throw new UnsupportedOperationException();
        }
    }

    private static Object convertValue(com.google.protobuf.Descriptors.FieldDescriptor fd, Object value) {
        if (value == null) {
            return null;
        }
        return switch (fd.getJavaType()) {
            case STRING -> value.toString();
            case INT -> ((Number) value).intValue();
            case LONG -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BOOLEAN -> (value instanceof Boolean b) ? b : Boolean.parseBoolean(value.toString());
            default -> value.toString();
        };
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
            HttpClient client = with.isRedirect() ?
                    HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build() :
                    HttpClient.newHttpClient();
            HttpResponse<String> resp = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            HTTPOutput out = with.getOutput() == null ? HTTPOutput.CONTENT : with.getOutput();
            switch (out) {
                case RAW -> ctx.set(RESULT, resp.body());
                case CONTENT -> {
                    try {
                        Object obj = MAPPER.readValue(resp.body(), Object.class);
                        ctx.set(RESULT, obj);
                    } catch (Exception e) {
                        ctx.set(RESULT, resp.body());
                    }
                }
                case RESPONSE -> {
                    java.util.Map<String, Object> map = new java.util.HashMap<>();
                    map.put("status", resp.statusCode());
                    map.put("headers", resp.headers().map());
                    map.put("body", resp.body());
                    ctx.set(RESULT, map);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void doGrpcCall(WorkflowContext ctx, CallGRPC t) {
        var with = t.getWith();
        if (with == null || with.getProto() == null || with.getService() == null) {
            return;
        }
        try {
            // Resolve proto endpoint
            Object ep = with.getProto().getEndpoint().get();
            String protoStr;
            if (ep instanceof URI u) {
                protoStr = u.toString();
            } else if (ep instanceof io.serverlessworkflow.api.types.EndpointConfiguration cfg) {
                Object uo = cfg.getUri().get();
                if (uo instanceof io.serverlessworkflow.api.types.UriTemplate ut && ut.getLiteralUri() != null) {
                    protoStr = ut.getLiteralUri().toString();
                } else {
                    protoStr = uo.toString();
                }
            } else {
                protoStr = ep.toString();
            }
            protoStr = resolveExpressions(ctx, protoStr);
            java.nio.file.Path protoPath = java.nio.file.Path.of(URI.create(protoStr));

            com.google.protobuf.DescriptorProtos.FileDescriptorSet fdset;
            if (protoPath.toString().endsWith(".desc") || protoPath.toString().endsWith(".pb")) {
                fdset = com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(
                        java.nio.file.Files.readAllBytes(protoPath));
            } else {
                java.nio.file.Path descPath = java.nio.file.Files.createTempFile("proto", ".desc");
                String protocExe = System.getenv().getOrDefault("PROTOC", System.getProperty("protoc", "/usr/bin/protoc"));
                Process proc = new ProcessBuilder(protocExe,
                        "-I", ".",
                        "--descriptor_set_out=" + descPath,
                        "--include_imports", protoPath.getFileName().toString())
                        .directory(protoPath.getParent().toFile()).start();
                if (proc.waitFor() != 0) {
                    String err = new String(proc.getErrorStream().readAllBytes());
                    throw new IllegalStateException("protoc failed: " + err);
                }
                fdset = com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(
                        java.nio.file.Files.readAllBytes(descPath));
            }

            java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protos = new java.util.HashMap<>();
            for (var fdp : fdset.getFileList()) {
                protos.put(fdp.getName(), fdp);
            }

            java.util.Map<String, com.google.protobuf.Descriptors.FileDescriptor> descs = new java.util.HashMap<>();
            for (var fdp : fdset.getFileList()) {
                buildFileDescriptor(fdp, protos, descs);
            }

            String svcName = with.getService().getName();
            com.google.protobuf.Descriptors.ServiceDescriptor svcDesc = null;
            for (var fd : descs.values()) {
                svcDesc = fd.findServiceByName(svcName.substring(svcName.lastIndexOf('.') + 1));
                if (svcDesc != null && (fd.getPackage() + "." + svcDesc.getName()).equals(svcName)) {
                    break;
                } else {
                    svcDesc = null;
                }
            }
            if (svcDesc == null) {
                log.warn("Service not found: {}", svcName);
                return;
            }

            var methodDesc = svcDesc.findMethodByName(with.getMethod());
            if (methodDesc == null) {
                log.warn("Method not found: {}", with.getMethod());
                return;
            }

            java.util.Map<String, Object> args = java.util.Collections.emptyMap();
            if (with.getArguments() != null) {
                args = with.getArguments().getAdditionalProperties();
            }

            com.google.protobuf.DynamicMessage.Builder reqBuilder =
                    com.google.protobuf.DynamicMessage.newBuilder(methodDesc.getInputType());
            for (var e : args.entrySet()) {
                var fd = methodDesc.getInputType().findFieldByName(e.getKey());
                if (fd != null) {
                    reqBuilder.setField(fd, convertValue(fd, e.getValue()));
                }
            }

            com.google.protobuf.DynamicMessage request = reqBuilder.build();

            int port = with.getService().getPort();
            if (port == 0) {
                port = 80;
            }
            io.grpc.ManagedChannel channel = io.grpc.ManagedChannelBuilder
                    .forAddress(with.getService().getHost(), port)
                    .usePlaintext()
                    .build();
            try {
                io.grpc.MethodDescriptor<com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage> md =
                        io.grpc.MethodDescriptor.<com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage>newBuilder()
                                .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName(svcName, with.getMethod()))
                                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                        com.google.protobuf.DynamicMessage.getDefaultInstance(methodDesc.getInputType())))
                                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                        com.google.protobuf.DynamicMessage.getDefaultInstance(methodDesc.getOutputType())))
                                .build();
                com.google.protobuf.DynamicMessage response = io.grpc.stub.ClientCalls.blockingUnaryCall(
                        channel, md, io.grpc.CallOptions.DEFAULT, request);
                String json = com.google.protobuf.util.JsonFormat.printer().print(response);
                try {
                    ctx.set(RESULT, MAPPER.readValue(json, Object.class));
                } catch (Exception e) {
                    ctx.set(RESULT, json);
                }
            } finally {
                channel.shutdownNow();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static com.google.protobuf.Descriptors.FileDescriptor buildFileDescriptor(
            com.google.protobuf.DescriptorProtos.FileDescriptorProto proto,
            java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protos,
            java.util.Map<String, com.google.protobuf.Descriptors.FileDescriptor> descs)
            throws java.lang.Exception {
        if (descs.containsKey(proto.getName())) {
            return descs.get(proto.getName());
        }
        java.util.List<com.google.protobuf.Descriptors.FileDescriptor> deps = new java.util.ArrayList<>();
        for (String depName : proto.getDependencyList()) {
            var depProto = protos.get(depName);
            if (depProto != null) {
                deps.add(buildFileDescriptor(depProto, protos, descs));
            }
        }
        com.google.protobuf.Descriptors.FileDescriptor fd =
                com.google.protobuf.Descriptors.FileDescriptor.buildFrom(proto,
                        deps.toArray(new com.google.protobuf.Descriptors.FileDescriptor[0]));
        descs.put(proto.getName(), fd);
        return fd;
    }
}
