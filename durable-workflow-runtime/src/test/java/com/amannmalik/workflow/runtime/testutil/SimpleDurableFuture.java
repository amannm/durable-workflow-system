package com.amannmalik.workflow.runtime.testutil;

import dev.restate.sdk.DurableFuture;

import java.util.concurrent.Executor;

public class SimpleDurableFuture<T> extends DurableFuture<T> {
    private final T value;

    public SimpleDurableFuture(T value) {
        this.value = value;
    }

    @Override
    protected dev.restate.sdk.endpoint.definition.AsyncResult<T> asyncResult() {
        return new SimpleAsyncResult<>(value);
    }

    @Override
    protected Executor serviceExecutor() {
        return Runnable::run;
    }
}
