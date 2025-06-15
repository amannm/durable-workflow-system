package com.amannmalik.workflow.runtime.testutil;

import dev.restate.common.Output;
import dev.restate.sdk.DurableFuture;
import dev.restate.sdk.InvocationHandle;

public class SimpleInvocationHandle<T> implements InvocationHandle<T> {
  private final String id;
  public boolean cancelled = false;

  public SimpleInvocationHandle(String id) {
    this.id = id;
  }

  @Override
  public String invocationId() {
    return id;
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @Override
  public DurableFuture<T> attach() {
    return new SimpleDurableFuture<>(null);
  }

  @Override
  public Output<T> getOutput() {
    return Output.ready(null);
  }
}
