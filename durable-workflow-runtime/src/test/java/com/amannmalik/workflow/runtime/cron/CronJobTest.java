package com.amannmalik.workflow.runtime.cron;

import dev.restate.sdk.*;
import dev.restate.common.Request;
import dev.restate.common.Output;
import dev.restate.sdk.common.HandlerRequest;
import dev.restate.sdk.common.RetryPolicy;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.TypeTag;
import dev.restate.sdk.common.TerminalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.*;

class CronJobTest {

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
        boolean cancelled = false;
        SimpleInvocationHandle(String id) { this.id = id; }
        @Override public String invocationId() { return id; }
        @Override public void cancel() { cancelled = true; }
        @Override public DurableFuture<T> attach() { return new SimpleDurableFuture<>(null); }
        @Override public Output<T> getOutput() { return Output.ready(null); }
    }

    static class FakeContext implements ObjectContext {
        Map<String,Object> state = new HashMap<>();
        List<Duration> delays = new ArrayList<>();
        int idCounter = 0;
        Map<String,SimpleInvocationHandle<?>> handles = new HashMap<>();
        @Override public HandlerRequest request() { return null; }
        @Override public <T, R> CallDurableFuture<R> call(Request<T, R> request) { return null; }
        @Override public <T, R> InvocationHandle<R> send(Request<T, R> request, Duration delay) { 
            String id = "inv-" + (idCounter++);
            delays.add(delay);
            SimpleInvocationHandle<R> h = new SimpleInvocationHandle<>(id);
            handles.put(id,h);
            return h;
        }
        @Override public <T, R> InvocationHandle<R> send(Request<T, R> request) {
            String id = "inv-" + (idCounter++);
            SimpleInvocationHandle<R> h = new SimpleInvocationHandle<>(id);
            handles.put(id, h);
            return h;
        }
        @Override public <R> InvocationHandle<R> invocationHandle(String id, TypeTag<R> typeTag) { return (InvocationHandle<R>) handles.get(id); }
        @Override public DurableFuture<Void> timer(String id, Duration duration) { return new SimpleDurableFuture<>(null); }
        @Override public <T> DurableFuture<T> runAsync(String name, TypeTag<T> typeTag, RetryPolicy policy, dev.restate.common.function.ThrowingSupplier<T> supplier) { try { return new SimpleDurableFuture<>(supplier.get()); } catch (Throwable t) { throw new RuntimeException(t); } }
        @Override public <T> Awakeable<T> awakeable(TypeTag<T> typeTag) { return null; }
        @Override public AwakeableHandle awakeableHandle(String id) { return null; }
        @Override public RestateRandom random() { throw new UnsupportedOperationException(); }
        @Override public String key() { return "job"; }
        @Override public <T> Optional<T> get(StateKey<T> key) { return Optional.ofNullable((T)state.get(key.name())); }
        @Override public Collection<String> stateKeys() { return state.keySet(); }
        @Override public void clear(StateKey<?> key) { state.remove(key.name()); }
        @Override public void clearAll() { state.clear(); }
        @Override public <T> void set(StateKey<T> key, T value) { state.put(key.name(), value); }
        @Override public void sleep(Duration d) {}
    }

    FakeContext ctx;
    CronJobRequest request;

    @BeforeEach
    void setup() {
        ctx = new FakeContext();
        request = new CronJobRequest("* * * * *", "S", "M", Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    void initiateSchedulesJob() {
        CronJobInfo info = CronJob.initiate(ctx, request);
        assertNotNull(info.nextExecutionId());
        assertTrue(ctx.get(StateKey.of("job-state", CronJobInfo.class)).isPresent());
        assertFalse(ctx.delays.isEmpty());
    }

    @Test
    void initiatingTwiceFails() {
        CronJob.initiate(ctx, request);
        assertThrows(TerminalException.class, () -> CronJob.initiate(ctx, request));
    }

    @Test
    void cancelClearsState() {
        CronJobInfo info = CronJob.initiate(ctx, request);
        CronJob.cancel(ctx);
        assertTrue(ctx.state.isEmpty());
        SimpleInvocationHandle<?> handle = (SimpleInvocationHandle<?>) ctx.handles.get(info.nextExecutionId());
        assertTrue(handle.cancelled);
    }

    @Test
    void executeReschedulesJob() {
        CronJobInfo info1 = CronJob.initiate(ctx, request);
        String firstId = info1.nextExecutionId();
        CronJob.execute(ctx);
        CronJobInfo info2 = ctx.get(StateKey.of("job-state", CronJobInfo.class)).orElseThrow();
        assertNotEquals(firstId, info2.nextExecutionId());
        assertEquals(2, ctx.delays.size());
    }

    @Test
    void invalidCronExpressionThrows() {
        CronJobRequest bad = new CronJobRequest("nope", "S", "M", Optional.empty(), Optional.empty(), Optional.empty());
        assertThrows(TerminalException.class, () -> CronJob.initiate(ctx, bad));
    }
}
