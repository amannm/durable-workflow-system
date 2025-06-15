package com.amannmalik.workflow.runtime.task.call;

import com.amannmalik.grpc.HelloReply;
import com.amannmalik.grpc.HelloRequest;
import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.serverlessworkflow.api.types.AsyncApiArguments;
import io.serverlessworkflow.api.types.AsyncApiMessagePayload;
import io.serverlessworkflow.api.types.AsyncApiOutboundMessage;
import io.serverlessworkflow.api.types.AsyncApiServer;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import dev.restate.sdk.common.StateKey;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallTask;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.OpenAPIArguments;
import io.serverlessworkflow.api.types.Endpoint;
import io.serverlessworkflow.api.types.EndpointConfiguration;
import io.serverlessworkflow.api.types.EndpointUri;
import io.serverlessworkflow.api.types.ExternalResource;
import io.serverlessworkflow.api.types.GRPCArguments;
import io.serverlessworkflow.api.types.HTTPArguments;
import io.serverlessworkflow.api.types.HTTPArguments.HTTPOutput;
import io.serverlessworkflow.api.types.UriTemplate;
import io.serverlessworkflow.api.types.WithGRPCArguments;
import io.serverlessworkflow.api.types.WithGRPCService;
import java.net.URI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
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
        server.stubFor(
                get(urlEqualTo("/test"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withHeader("X-Test", "A")
                                        .withBody("{\"foo\":\"bar\"}")));
        String url = server.url("/test");
        ctx.set(StateKey.of("endpoint", String.class), url);
        Endpoint ep = new Endpoint();
        ep.setRuntimeExpression("${endpoint}");

        for (HTTPOutput out : HTTPOutput.values()) {
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
                case CONTENT -> assertTrue(
                        result instanceof Map && "bar".equals(((Map<?, ?>) result).get("foo")));
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
        MethodDescriptor<HelloRequest, HelloReply> md =
                MethodDescriptor
                        .<HelloRequest, HelloReply>newBuilder()
                        .setType(MethodType.UNARY)
                        .setFullMethodName(
                                MethodDescriptor.generateFullMethodName("GreeterApi.Greeter", "SayHello"))
                        .setRequestMarshaller(
                                ProtoUtils.marshaller(
                                        HelloRequest.getDefaultInstance()))
                        .setResponseMarshaller(
                                ProtoUtils.marshaller(
                                        HelloReply.getDefaultInstance()))
                        .build();

        ServerServiceDefinition service =
                ServerServiceDefinition.builder("GreeterApi.Greeter")
                        .addMethod(
                                md,
                                ServerCalls.asyncUnaryCall(
                                        (req, resp) -> {
                                            HelloReply reply =
                                                    HelloReply.newBuilder()
                                                            .setMessage("Hello " + req.getName())
                                                            .build();
                                            resp.onNext(reply);
                                            resp.onCompleted();
                                        }))
                        .build();

        Server grpcServer =
                ServerBuilder.forPort(0).addService(service).build().start();
        int grpcPort = grpcServer.getPort();

        // Prepare CallGRPC task
        ExternalResource proto = new ExternalResource();
        Endpoint ep = new Endpoint();
        EndpointConfiguration cfg = new EndpointConfiguration();
        Path protoPath = Path.of("src/test/resources/greet.desc");
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
        assertEquals("Hello Codex", ((Map<?, ?>) result).get("message"));

        grpcServer.shutdownNow();
    }

    @Test
    void asyncApiPublish() {
        String doc =
                "asyncapi: '2.6.0'\n"
                        + "servers:\n"
                        + "  test:\n"
                        + "    url: "
                        + server.url("")
                        + "\n    protocol: http\n"
                        + "channels:\n"
                        + "  /publish:\n"
                        + "    publish:\n"
                        + "      operationId: sendMsg\n";
        server.stubFor(
                get(urlEqualTo("/doc.yaml"))
                        .willReturn(
                                aResponse()
                                        .withHeader("Content-Type", "text/yaml")
                                        .withBody(doc)));
        server.stubFor(post(urlEqualTo("/publish")).willReturn(aResponse().withStatus(204)));

        ExternalResource res = new ExternalResource();
        Endpoint ep = new Endpoint();
        EndpointConfiguration cfg = new EndpointConfiguration();
        EndpointUri uri = new EndpointUri();
        UriTemplate ut = new UriTemplate();
        ut.setLiteralUri(URI.create(server.url("/doc.yaml")));
        uri.setLiteralEndpointURI(ut);
        cfg.setUri(uri);
        ep.setEndpointConfiguration(cfg);
        res.setEndpoint(ep);

        AsyncApiOutboundMessage msg = new AsyncApiOutboundMessage();
        AsyncApiMessagePayload payload = new AsyncApiMessagePayload();
        payload.setAdditionalProperty("foo", "bar");
        msg.setPayload(payload);

        AsyncApiArguments args = new AsyncApiArguments();
        args.setDocument(res);
        args.setChannel("/publish");
        args.setMessage(msg);
        AsyncApiServer serverDef = new AsyncApiServer();
        serverDef.setName("test");
        args.setServer(serverDef);

        CallAsyncAPI ca = new CallAsyncAPI();
        ca.setCall("asyncapi");
        ca.setWith(args);

        CallTask task = new CallTask();
        task.setCallAsyncAPI(ca);

        CallTaskService.execute(ctx, task);

        server.verify(postRequestedFor(urlEqualTo("/publish")));
    }

    @Test
    void openApiCall() {
        String doc =
                "openapi: 3.0.0\n"
                        + "servers:\n"
                        + "  - url: "
                        + server.url("")
                        + "\n"
                        + "paths:\n"
                        + "  /pets:\n"
                        + "    get:\n"
                        + "      operationId: listPets\n";
        server.stubFor(
                get(urlEqualTo("/openapi.yaml"))
                        .willReturn(
                                aResponse()
                                        .withHeader("Content-Type", "text/yaml")
                                        .withBody(doc)));
        server.stubFor(get(urlEqualTo("/pets"))
                .willReturn(aResponse().withStatus(200).withBody("[{}]")));

        ExternalResource res = new ExternalResource();
        Endpoint ep = new Endpoint();
        EndpointConfiguration cfg = new EndpointConfiguration();
        EndpointUri uri = new EndpointUri();
        UriTemplate ut = new UriTemplate();
        ut.setLiteralUri(URI.create(server.url("/openapi.yaml")));
        uri.setLiteralEndpointURI(ut);
        cfg.setUri(uri);
        ep.setEndpointConfiguration(cfg);
        res.setEndpoint(ep);

        OpenAPIArguments args = new OpenAPIArguments();
        args.setDocument(res);
        args.setOperationId("listPets");

        CallOpenAPI co = new CallOpenAPI();
        co.setCall("openapi");
        co.setWith(args);

        CallTask task = new CallTask();
        task.setCallOpenAPI(co);

        CallTaskService.execute(ctx, task);

        Object result = ctx.get(CallTaskService.RESULT).orElse(null);
        assertNotNull(result);
        assertTrue(result instanceof List);

        server.verify(getRequestedFor(urlEqualTo("/pets")));
    }

    static class FakeContext extends FakeWorkflowContext {
    }
}
