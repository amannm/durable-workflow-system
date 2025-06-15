package com.amannmalik.workflow.runtime.task.call.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallGRPC;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcCallHandler implements CallHandler<CallGRPC> {
  private static final Logger log = LoggerFactory.getLogger(GrpcCallHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final StateKey<Object> resultKey;

  public GrpcCallHandler(StateKey<Object> resultKey) {
    this.resultKey = resultKey;
  }

  private static Object convertValue(
      com.google.protobuf.Descriptors.FieldDescriptor fd, Object value) {
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

  private static com.google.protobuf.Descriptors.FileDescriptor buildFileDescriptor(
      com.google.protobuf.DescriptorProtos.FileDescriptorProto proto,
      java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protos,
      java.util.Map<String, com.google.protobuf.Descriptors.FileDescriptor> descs)
      throws java.lang.Exception {
    if (descs.containsKey(proto.getName())) {
      return descs.get(proto.getName());
    }
    java.util.List<com.google.protobuf.Descriptors.FileDescriptor> deps =
        new java.util.ArrayList<>();
    for (String depName : proto.getDependencyList()) {
      var depProto = protos.get(depName);
      if (depProto != null) {
        deps.add(buildFileDescriptor(depProto, protos, descs));
      }
    }
    com.google.protobuf.Descriptors.FileDescriptor fd =
        com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
            proto, deps.toArray(new com.google.protobuf.Descriptors.FileDescriptor[0]));
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
      } else if (ep instanceof io.serverlessworkflow.api.types.EndpointConfiguration cfg) {
        Object uo = cfg.getUri().get();
        if (uo instanceof io.serverlessworkflow.api.types.UriTemplate ut
            && ut.getLiteralUri() != null) {
          protoStr = ut.getLiteralUri().toString();
        } else {
          protoStr = uo.toString();
        }
      } else {
        protoStr = ep.toString();
      }
      protoStr = ExpressionResolver.resolveExpressions(ctx, protoStr);
      java.nio.file.Path protoPath = java.nio.file.Path.of(URI.create(protoStr));

      com.google.protobuf.DescriptorProtos.FileDescriptorSet fdset;
      if (protoPath.toString().endsWith(".desc") || protoPath.toString().endsWith(".pb")) {
        fdset =
            com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(
                java.nio.file.Files.readAllBytes(protoPath));
      } else {
        java.nio.file.Path descPath = java.nio.file.Files.createTempFile("proto", ".desc");
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
            com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom(
                java.nio.file.Files.readAllBytes(descPath));
      }

      java.util.Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> protos =
          new java.util.HashMap<>();
      for (var fdp : fdset.getFileList()) {
        protos.put(fdp.getName(), fdp);
      }

      java.util.Map<String, com.google.protobuf.Descriptors.FileDescriptor> descs =
          new java.util.HashMap<>();
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
      io.grpc.ManagedChannel channel =
          io.grpc.ManagedChannelBuilder.forAddress(with.getService().getHost(), port)
              .usePlaintext()
              .build();
      try {
        io.grpc.MethodDescriptor<
                com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage>
            md =
                io.grpc.MethodDescriptor
                    .<com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage>
                        newBuilder()
                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(
                        io.grpc.MethodDescriptor.generateFullMethodName(svcName, with.getMethod()))
                    .setRequestMarshaller(
                        io.grpc.protobuf.ProtoUtils.marshaller(
                            com.google.protobuf.DynamicMessage.getDefaultInstance(
                                methodDesc.getInputType())))
                    .setResponseMarshaller(
                        io.grpc.protobuf.ProtoUtils.marshaller(
                            com.google.protobuf.DynamicMessage.getDefaultInstance(
                                methodDesc.getOutputType())))
                    .build();
        com.google.protobuf.DynamicMessage response =
            io.grpc.stub.ClientCalls.blockingUnaryCall(
                channel, md, io.grpc.CallOptions.DEFAULT, request);
        String json = com.google.protobuf.util.JsonFormat.printer().print(response);
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
