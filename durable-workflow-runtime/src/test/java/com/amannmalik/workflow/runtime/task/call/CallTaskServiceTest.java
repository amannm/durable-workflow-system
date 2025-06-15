package com.amannmalik.workflow.runtime.task.call;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.restate.common.Output;
import dev.restate.common.Request;
import dev.restate.sdk.Awakeable;
import dev.restate.sdk.AwakeableHandle;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.DurablePromise;
import dev.restate.sdk.DurablePromiseHandle;
import dev.restate.sdk.InvocationHandle;
import dev.restate.sdk.RestateRandom;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.TypeTag;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CallTaskServiceTest {

    private WireMockServer server;

    static class SimpleAsyncResult<T> implements dev.restate.sdk.endpoint.definition.AsyncResult<T> {
        private final T value;
        SimpleAsyncResult(T v) { this.value = v; }
        @Override public CompletableFuture<T> poll() { return CompletableFuture.completedFuture(value); }
        @Override public dev.restate.sdk.endpoint.definition.HandlerContext ctx() { return null; }
        @Override public <U> dev.restate.sdk.endpoint.definition.AsyncResult<U> map(dev.restate.common.function.ThrowingFunction<T, CompletableFuture<U>> f, dev.restate.common.function.ThrowingFunction<dev.restate.sdk.common.TerminalException, CompletableFuture<U>> g) { return new SimpleAsyncResult<>(null); }
    }

    static class SimpleDurableFuture<T> extends DurableFuture<T> {
        private final T value;
        SimpleDurableFuture(T v) { this.value = v; }
        @Override protected dev.restate.sdk.endpoint.definition.AsyncResult<T> asyncResult() { return new SimpleAsyncResult<>(value); }
        @Override protected Executor serviceExecutor() { return Runnable::run; }
    }

    static class SimpleInvocationHandle<T> implements InvocationHandle<T> {
        private final String id;
        SimpleInvocationHandle(String id) { this.id = id; }
        @Override public String invocationId() { return id; }
        @Override public void cancel() { }
        @Override public DurableFuture<T> attach() { return new SimpleDurableFuture<>(null); }
        @Override public Output<T> getOutput() { return Output.ready(null); }
    }

    static class FakeContext implements WorkflowContext {
        Map<String,Object> state = new HashMap<>();
        @Override public HandlerRequest request() { return null; }
        @Override public <T, R> CallDurableFuture<R> call(Request<T, R> request) { return null; }
        @Override public <T, R> InvocationHandle<R> send(Request<T, R> request, Duration delay) { return new SimpleInvocationHandle<>("id"); }
        @Override public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) { return new SimpleInvocationHandle<>(id); }
        @Override public DurableFuture<Void> timer(String id, Duration duration) { return new SimpleDurableFuture<>(null); }
        @Override public <T> DurableFuture<T> runAsync(String name, TypeTag<T> typeTag, RetryPolicy policy, dev.restate.common.function.ThrowingSupplier<T> supplier) { return new SimpleDurableFuture<>(null); }
        @Override public <T> Awakeable<T> awakeable(TypeTag<T> typeTag) { return null; }
        @Override public AwakeableHandle awakeableHandle(String id) { return null; }
        @Override public RestateRandom random() { throw new UnsupportedOperationException(); }
        @Override public <T> DurablePromise<T> promise(dev.restate.sdk.common.DurablePromiseKey<T> key) { return null; }
        @Override public <T> DurablePromiseHandle<T> promiseHandle(dev.restate.sdk.common.DurablePromiseKey<T> key) { return null; }
        @Override public String key() { return "key"; }
        @Override public <T> Optional<T> get(StateKey<T> key) { return Optional.ofNullable((T)state.get(key.name())); }
        @Override public Collection<String> stateKeys() { return state.keySet(); }
        @Override public void clear(StateKey<?> key) { state.remove(key.name()); }
        @Override public void clearAll() { state.clear(); }
        @Override public <T> void set(StateKey<T> key, T value) { state.put(key.name(), value); }
        @Override public void sleep(Duration d) { }
    }

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
                case CONTENT -> assertTrue(result instanceof Map && "bar".equals(((Map<?,?>)result).get("foo")));
                case RESPONSE -> {
                    assertTrue(result instanceof Map);
                    Map<?,?> map = (Map<?,?>) result;
                    assertEquals(200, map.get("status"));
                    assertEquals("{\"foo\":\"bar\"}", map.get("body"));
                    Map<?,?> headers = (Map<?,?>) map.get("headers");
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
        assertTrue(result instanceof java.util.Map);
        assertEquals("Hello Codex", ((java.util.Map<?, ?>) result).get("message"));

        grpcServer.shutdownNow();
    }
}
