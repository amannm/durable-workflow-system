package com.amannmalik.workflow.runtime.testutil;

import dev.restate.common.Request;
import dev.restate.common.function.ThrowingSupplier;
import dev.restate.sdk.Awakeable;
import dev.restate.sdk.AwakeableHandle;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.DurablePromise;
import dev.restate.sdk.DurablePromiseHandle;
import dev.restate.sdk.InvocationHandle;
import dev.restate.sdk.RestateRandom;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.DurablePromiseKey;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.TypeTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FakeWorkflowContext implements WorkflowContext {
    public final Map<String, Object> state = new HashMap<>();
    public final List<Duration> sleeps = new ArrayList<>();
    public final List<Duration> delays = new ArrayList<>();
    public final Map<String, SimpleInvocationHandle<?>> handles = new HashMap<>();
    private int counter = 0;
    private final String key;

    public FakeWorkflowContext() {
        this("key");
    }

    public FakeWorkflowContext(String key) {
        this.key = key;
    }

    @Override
    public HandlerRequest request() {
        return null;
    }

    @Override
    public <T, R> CallDurableFuture<R> call(Request<T, R> request) {
        return null;
    }

    @Override
    public <T, R> InvocationHandle<R> send(Request<T, R> request, Duration delay) {
        String id = "inv-" + (counter++);
        SimpleInvocationHandle<R> h = new SimpleInvocationHandle<>(id);
        handles.put(id, h);
        delays.add(delay);
        return h;
    }

    @Override
    public <T, R> InvocationHandle<R> send(Request<T, R> request) {
        String id = "inv-" + (counter++);
        SimpleInvocationHandle<R> h = new SimpleInvocationHandle<>(id);
        handles.put(id, h);
        return h;
    }

    @Override
    public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) {
        return (InvocationHandle<R>) handles.getOrDefault(id, new SimpleInvocationHandle<>(id));
    }

    @Override
    public DurableFuture<Void> timer(String id, Duration duration) {
        return new SimpleDurableFuture<>(null);
    }

    @Override
    public <T> DurableFuture<T> runAsync(
            String name,
            TypeTag<T> typeTag,
            RetryPolicy policy,
            ThrowingSupplier<T> supplier) {
        try {
            if (supplier == null) {
                return new SimpleDurableFuture<>(null);
            }
            return new SimpleDurableFuture<>(supplier.get());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public <T> Awakeable<T> awakeable(TypeTag<T> typeTag) {
        return null;
    }

    @Override
    public AwakeableHandle awakeableHandle(String id) {
        return null;
    }

    @Override
    public RestateRandom random() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> DurablePromise<T> promise(DurablePromiseKey<T> key) {
        return null;
    }

    @Override
    public <T> DurablePromiseHandle<T> promiseHandle(
            DurablePromiseKey<T> key) {
        return null;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public <T> Optional<T> get(StateKey<T> key) {
        return Optional.ofNullable((T) state.get(key.name()));
    }

    @Override
    public Collection<String> stateKeys() {
        return state.keySet();
    }

    @Override
    public void clear(StateKey<?> key) {
        state.remove(key.name());
    }

    @Override
    public void clearAll() {
        state.clear();
    }

    @Override
    public <T> void set(StateKey<T> key, T value) {
        state.put(key.name(), value);
    }

    @Override
    public void sleep(Duration d) {
        sleeps.add(d);
    }
}
