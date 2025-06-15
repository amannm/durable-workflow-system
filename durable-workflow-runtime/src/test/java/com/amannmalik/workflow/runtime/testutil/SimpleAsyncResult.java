package com.amannmalik.workflow.runtime.testutil;

import dev.restate.sdk.endpoint.definition.AsyncResult;
import dev.restate.sdk.endpoint.definition.HandlerContext;
import java.util.concurrent.CompletableFuture;

public class SimpleAsyncResult<T> implements AsyncResult<T> {
  private final T value;

  public SimpleAsyncResult(T value) {
    this.value = value;
  }

  @Override
  public CompletableFuture<T> poll() {
    return CompletableFuture.completedFuture(value);
  }

  @Override
  public HandlerContext ctx() {
    return null;
  }

  @Override
  public <U> AsyncResult<U> map(
      dev.restate.common.function.ThrowingFunction<T, CompletableFuture<U>> f,
      dev.restate.common.function.ThrowingFunction<
              dev.restate.sdk.common.TerminalException, CompletableFuture<U>>
          g) {
    return new SimpleAsyncResult<>(null);
  }
}
