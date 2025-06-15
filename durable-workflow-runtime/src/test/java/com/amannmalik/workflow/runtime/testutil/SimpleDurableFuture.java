package com.amannmalik.workflow.runtime.testutil;

import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.endpoint.definition.AsyncResult;

import java.util.concurrent.Executor;

public class SimpleDurableFuture<T> extends DurableFuture<T> {
    private final T value;

    public SimpleDurableFuture(T value) {
        this.value = value;
    }

    @Override
    protected AsyncResult<T> asyncResult() {
        return new SimpleAsyncResult<>(value);
    }

    @Override
    protected Executor serviceExecutor() {
        return Runnable::run;
    }
}
