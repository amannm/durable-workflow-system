package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.*;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.common.StateKey;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.common.Output;
import dev.restate.serde.TypeTag;
import io.serverlessworkflow.api.types.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;

class SetTaskServiceTest {
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

    @Test
    void setsVariousTypes() {
        FakeContext ctx = new FakeContext();
        SetTask st = new SetTask();
        io.serverlessworkflow.api.types.Set s = new io.serverlessworkflow.api.types.Set();
        s.setString("foo");
        st.setSet(s);
        SetTaskService.execute(ctx, st);
        assertEquals("foo", ctx.get(StateKey.of("foo", String.class)).orElse(null));
    }
}
