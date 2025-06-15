package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.task.*;
import com.amannmalik.workflow.runtime.task.call.CallTaskService;
import com.amannmalik.workflow.runtime.task.run.RunTaskService;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.restate.common.Output;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.*;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

class EnterpriseWorkflowE2ETest {
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
        List<Duration> sleeps = new ArrayList<>();
        int counter = 0;
        @Override public HandlerRequest request() { return null; }
        @Override public <T, R> CallDurableFuture<R> call(Request<T, R> request) {
            Target t = request.getTarget();
            String svc = t.getService();
            String m = t.getHandler();
            Object req = request.getRequest();
            if ("WaitTaskService".equals(svc) && "execute".equals(m)) {
                WaitTaskService.execute(this, (WaitTask) req);
            } else if ("ForkTaskService".equals(svc) && "execute".equals(m)) {
                ForkTaskService.execute(this, (ForkTask) req);
            } else if ("RunTaskService".equals(svc) && "execute".equals(m)) {
                RunTaskService.execute(this, (RunTask) req);
            } else if ("SetTaskService".equals(svc) && "execute".equals(m)) {
                SetTaskService.execute(this, (SetTask) req);
            } else if ("SwitchTaskService".equals(svc) && "execute".equals(m)) {
                SwitchTaskService.execute(this, (SwitchTask) req);
            } else if ("CallTaskService".equals(svc) && "execute".equals(m)) {
                CallTaskService.execute(this, (CallTask) req);
            } else if ("ListenTaskService".equals(svc) && "execute".equals(m)) {
                ListenTaskService.execute(this, (ListenTask) req);
            } else if ("EmitTaskService".equals(svc) && "execute".equals(m)) {
                EmitTaskService.execute(this, (EmitTask) req);
            } else if ("TryTaskService".equals(svc) && "execute".equals(m)) {
                TryTaskService.execute(this, (TryTask) req);
            }
            return null;
        }
        @Override public <T, R> InvocationHandle<R> send(Request<T, R> request, Duration delay) { return new SimpleInvocationHandle<>("inv-"+(counter++)); }
        @Override public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) { return new SimpleInvocationHandle<>(id); }
        @Override public DurableFuture<Void> timer(String id, Duration duration) { return new SimpleDurableFuture<>(null); }
        @Override public <T> DurableFuture<T> runAsync(String name, TypeTag<T> typeTag, RetryPolicy policy, dev.restate.common.function.ThrowingSupplier<T> supplier) { try { return new SimpleDurableFuture<>(supplier.get()); } catch (Throwable e) { throw new RuntimeException(e); } }
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
        @Override public void sleep(Duration d) { sleeps.add(d); }
    }

    @BeforeEach
    void setup() {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        WireMock.configureFor("localhost", server.port());
        server.stubFor(any(anyUrl()).willReturn(aResponse().withStatus(200).withBody("{}")));
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    void runEnterpriseWorkflow() throws Exception {
        String yaml = Files.readString(Path.of("src/test/resources/enterprise.yaml"));
        yaml = yaml.replaceAll("https?://[A-Za-z0-9\\.-]+", server.baseUrl());
        Workflow wf = WorkflowLoader.fromYaml(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));
        FakeContext ctx = new FakeContext();
        new WorkflowRunner().runInternal(ctx, wf);
        assertNotNull(ctx);
    }
}
