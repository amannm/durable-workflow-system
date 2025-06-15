package com.amannmalik.workflow.runtime.task;

import io.serverlessworkflow.api.types.Error;

/**
 * Exception thrown when a workflow error is raised.
 */
public class WorkflowErrorException extends RuntimeException {

    private final Error error;

    public WorkflowErrorException(Error error) {
        super(extractTitle(error));
        this.error = error;
    }

    private static String extractTitle(Error error) {
        if (error == null) {
            return null;
        }
        var title = error.getTitle();
        return title != null ? title.getLiteralErrorTitle() : null;
    }

    public Error getError() {
        return error;
    }
}
