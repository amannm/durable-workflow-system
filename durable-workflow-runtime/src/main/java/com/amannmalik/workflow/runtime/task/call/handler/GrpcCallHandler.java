package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.util.JsonFormat;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.EndpointConfiguration;
import io.serverlessworkflow.api.types.UriTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GrpcCallHandler implements CallHandler<CallGRPC> {
    private static final Logger log = LoggerFactory.getLogger(GrpcCallHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final StateKey<Object> resultKey;

    public GrpcCallHandler(StateKey<Object> resultKey) {
        this.resultKey = resultKey;
    }

    private static Object convertValue(
            FieldDescriptor fd, Object value) {
        if (value == null) {
            return null;
        }
        return switch (fd.getJavaType()) {
            case INT -> ((Number) value).intValue();
            case LONG -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BOOLEAN -> (value instanceof Boolean b) ? b : Boolean.parseBoolean(value.toString());
            default -> value.toString();
        };
    }

    private static FileDescriptor buildFileDescriptor(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> protos,
            Map<String, FileDescriptor> descs)
            throws Exception {
        if (descs.containsKey(proto.getName())) {
            return descs.get(proto.getName());
        }
        List<FileDescriptor> deps =
                new ArrayList<>();
        for (String depName : proto.getDependencyList()) {
            var depProto = protos.get(depName);
            if (depProto != null) {
                deps.add(buildFileDescriptor(depProto, protos, descs));
            }
        }
        FileDescriptor fd =
                FileDescriptor.buildFrom(
                        proto, deps.toArray(new FileDescriptor[0]));
        descs.put(proto.getName(), fd);
        return fd;
    }

    @Override
    public void handle(WorkflowContext ctx, CallGRPC t) {
        var with = t.getWith();
        if (with == null || with.getProto() == null || with.getService() == null) {
            return;
        }
        try {
            Object ep = with.getProto().getEndpoint().get();
            String protoStr;
            if (ep instanceof URI u) {
                protoStr = u.toString();
            } else if (ep instanceof EndpointConfiguration cfg) {
                Object uo = cfg.getUri().get();
                if (uo instanceof UriTemplate ut
                        && ut.getLiteralUri() != null) {
                    protoStr = ut.getLiteralUri().toString();
                } else {
                    protoStr = uo.toString();
                }
            } else {
                protoStr = ep.toString();
            }
            protoStr =
                    ExpressionResolver.resolveExpressions(ctx, protoStr)
                            .orElseThrow();
            Path protoPath = Path.of(URI.create(protoStr));

            FileDescriptorSet fdset;
            if (protoPath.toString().endsWith(".desc") || protoPath.toString().endsWith(".pb")) {
                fdset =
                        FileDescriptorSet.parseFrom(
                                Files.readAllBytes(protoPath));
            } else {
                Path descPath = Files.createTempFile("proto", ".desc");
                String protocExe =
                        System.getenv().getOrDefault("PROTOC", System.getProperty("protoc", "/usr/bin/protoc"));
                Process proc =
                        new ProcessBuilder(
                                protocExe,
                                "-I",
                                ".",
                                "--descriptor_set_out=" + descPath,
                                "--include_imports",
                                protoPath.getFileName().toString())
                                .directory(protoPath.getParent().toFile())
                                .start();
                if (proc.waitFor() != 0) {
                    String err = new String(proc.getErrorStream().readAllBytes());
                    throw new IllegalStateException("protoc failed: " + err);
                }
                fdset =
                        FileDescriptorSet.parseFrom(
                                Files.readAllBytes(descPath));
            }

            Map<String, FileDescriptorProto> protos =
                    new HashMap<>();
            for (var fdp : fdset.getFileList()) {
                protos.put(fdp.getName(), fdp);
            }

            Map<String, FileDescriptor> descs =
                    new HashMap<>();
            for (var fdp : fdset.getFileList()) {
                buildFileDescriptor(fdp, protos, descs);
            }

            String svcName = with.getService().getName();
            ServiceDescriptor svcDesc = null;
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

            Map<String, Object> args = Collections.emptyMap();
            if (with.getArguments() != null) {
                args = with.getArguments().getAdditionalProperties();
            }

            Builder reqBuilder =
                    DynamicMessage.newBuilder(methodDesc.getInputType());
            for (var e : args.entrySet()) {
                var fd = methodDesc.getInputType().findFieldByName(e.getKey());
                if (fd != null) {
                    reqBuilder.setField(fd, convertValue(fd, e.getValue()));
                }
            }

            DynamicMessage request = reqBuilder.build();

            int port = with.getService().getPort();
            if (port == 0) {
                port = 80;
            }
            ManagedChannel channel =
                    ManagedChannelBuilder.forAddress(with.getService().getHost(), port)
                            .usePlaintext()
                            .build();
            try {
                MethodDescriptor<
                        DynamicMessage, DynamicMessage>
                        md =
                        MethodDescriptor
                                .<DynamicMessage, DynamicMessage>
                                        newBuilder()
                                .setType(MethodType.UNARY)
                                .setFullMethodName(
                                        MethodDescriptor.generateFullMethodName(svcName, with.getMethod()))
                                .setRequestMarshaller(
                                        ProtoUtils.marshaller(
                                                DynamicMessage.getDefaultInstance(
                                                        methodDesc.getInputType())))
                                .setResponseMarshaller(
                                        ProtoUtils.marshaller(
                                                DynamicMessage.getDefaultInstance(
                                                        methodDesc.getOutputType())))
                                .build();
                DynamicMessage response =
                        ClientCalls.blockingUnaryCall(
                                channel, md, CallOptions.DEFAULT, request);
                String json = JsonFormat.printer().print(response);
                try {
                    ctx.set(resultKey, MAPPER.readValue(json, Object.class));
                } catch (Exception e) {
                    ctx.set(resultKey, json);
                }
            } finally {
                channel.shutdownNow();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
