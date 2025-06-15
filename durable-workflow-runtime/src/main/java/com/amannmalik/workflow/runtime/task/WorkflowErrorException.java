package com.amannmalik.workflow.runtime.task;

import io.serverlessworkflow.api.types.Error;

/** Exception thrown when a workflow error is raised. */
public class WorkflowErrorException extends RuntimeException {

  private final Error error;

  public WorkflowErrorException(Error error) {
    super(
        error != null && error.getTitle() != null ? error.getTitle().getLiteralErrorTitle() : null);
    this.error = error;
  }

  public Error getError() {
    return error;
  }
}
