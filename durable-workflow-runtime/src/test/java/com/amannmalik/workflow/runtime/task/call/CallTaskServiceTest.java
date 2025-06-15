package com.amannmalik.workflow.runtime.task.call;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.Endpoint;
import io.serverlessworkflow.api.types.EndpointConfiguration;
import io.serverlessworkflow.api.types.EndpointUri;
import io.serverlessworkflow.api.types.ExternalResource;
import io.serverlessworkflow.api.types.GRPCArguments;
import io.serverlessworkflow.api.types.HTTPArguments;
import io.serverlessworkflow.api.types.UriTemplate;
import io.serverlessworkflow.api.types.WithGRPCArguments;
import io.serverlessworkflow.api.types.WithGRPCService;
import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CallTaskServiceTest {

    private WireMockServer server;
    private FakeContext ctx;

    @BeforeEach
    void setup() {
        ctx = new FakeContext();
        server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        WireMock.configureFor("localhost", server.port());
    }

    @AfterEach
    void teardown() {
        server.stop();
    }

    @Test
    void captureDifferentOutputs() {
        server.stubFor(get(urlEqualTo("/test")).willReturn(aResponse().withStatus(200).withHeader("X-Test", "A").withBody("{\"foo\":\"bar\"}")));
        String url = server.url("/test");
        ctx.set(StateKey.of("endpoint", String.class), url);
        Endpoint ep = new Endpoint();
        ep.setRuntimeExpression("${endpoint}");

        for (HTTPArguments.HTTPOutput out : HTTPArguments.HTTPOutput.values()) {
            HTTPArguments args = new HTTPArguments();
            args.setMethod("GET");
            args.setEndpoint(ep);
            args.setOutput(out);
            CallHTTP ch = new CallHTTP();
            ch.setCall("http");
            ch.setWith(args);
            CallTask task = new CallTask();
            task.setCallHTTP(ch);
            CallTaskService.execute(ctx, task);
            Object result = ctx.get(CallTaskService.RESULT).orElse(null);
            assertNotNull(result);
            switch (out) {
                case RAW -> assertEquals("{\"foo\":\"bar\"}", result);
                case CONTENT -> assertTrue(result instanceof Map && "bar".equals(((Map<?, ?>) result).get("foo")));
                case RESPONSE -> {
                    assertInstanceOf(Map.class, result);
                    Map<?, ?> map = (Map<?, ?>) result;
                    assertEquals(200, map.get("status"));
                    assertEquals("{\"foo\":\"bar\"}", map.get("body"));
                    Map<?, ?> headers = (Map<?, ?>) map.get("headers");
                    assertTrue(headers.containsKey("X-Test"));
                }
            }
            ctx.clear(CallTaskService.RESULT);
        }
    }

    @Test
    void grpcCall() throws Exception {
        // Build simple gRPC server
        io.grpc.MethodDescriptor<com.amannmalik.grpc.HelloRequest, com.amannmalik.grpc.HelloReply> md =
                io.grpc.MethodDescriptor.<com.amannmalik.grpc.HelloRequest, com.amannmalik.grpc.HelloReply>newBuilder()
                        .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName("GreeterApi.Greeter", "SayHello"))
                        .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.amannmalik.grpc.HelloRequest.getDefaultInstance()))
                        .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.amannmalik.grpc.HelloReply.getDefaultInstance()))
                        .build();

        io.grpc.ServerServiceDefinition service = io.grpc.ServerServiceDefinition.builder("GreeterApi.Greeter")
                .addMethod(md, io.grpc.stub.ServerCalls.asyncUnaryCall((req, resp) -> {
                    com.amannmalik.grpc.HelloReply reply = com.amannmalik.grpc.HelloReply.newBuilder()
                            .setMessage("Hello " + req.getName()).build();
                    resp.onNext(reply);
                    resp.onCompleted();
                })).build();

        io.grpc.Server grpcServer = io.grpc.ServerBuilder.forPort(0).addService(service).build().start();
        int grpcPort = grpcServer.getPort();

        // Prepare CallGRPC task
        ExternalResource proto = new ExternalResource();
        Endpoint ep = new Endpoint();
        EndpointConfiguration cfg = new EndpointConfiguration();
        java.nio.file.Path protoPath = java.nio.file.Path.of("src/test/resources/greet.desc");
        EndpointUri euri = new EndpointUri();
        UriTemplate ut = new UriTemplate();
        ut.setLiteralUri(protoPath.toUri());
        euri.setLiteralEndpointURI(ut);
        cfg.setUri(euri);
        ep.setEndpointConfiguration(cfg);
        proto.setEndpoint(ep);

        WithGRPCService svc = new WithGRPCService();
        svc.setName("GreeterApi.Greeter");
        svc.setHost("localhost");
        svc.setPort(grpcPort);

        WithGRPCArguments wa = new WithGRPCArguments();
        wa.setAdditionalProperty("name", "Codex");

        GRPCArguments args = new GRPCArguments();
        args.setProto(proto);
        args.setService(svc);
        args.setMethod("SayHello");
        args.setArguments(wa);

        CallGRPC cg = new CallGRPC();
        cg.setCall("grpc");
        cg.setWith(args);

        CallTask task = new CallTask();
        task.setCallGRPC(cg);

        CallTaskService.execute(ctx, task);

        Object result = ctx.get(CallTaskService.RESULT).orElse(null);
        assertInstanceOf(Map.class, result);
        assertEquals("Hello Codex", ((java.util.Map<?, ?>) result).get("message"));

        grpcServer.shutdownNow();
    }

    static class FakeContext extends FakeWorkflowContext {
    }
}
