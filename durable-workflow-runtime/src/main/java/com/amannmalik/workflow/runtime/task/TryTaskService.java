package com.amannmalik.workflow.runtime.task;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TryTask;
import io.serverlessworkflow.api.types.TryTaskCatch;
import io.serverlessworkflow.api.types.ErrorFilter;
import io.serverlessworkflow.api.types.Error;

import com.amannmalik.workflow.runtime.task.WorkflowErrorException;

import java.util.List;

public class TryTaskService {

    public static final ServiceDefinition DEFINITION = DefinitionHelper.taskService(
            TryTaskService.class,
            TryTask.class,
            TryTaskService::execute
    );

    public static void execute(WorkflowContext ctx, TryTask task) {
        java.util.List<TaskItem> aTry = task.getTry();
        TryTaskCatch aCatch = task.getCatch();
        try {
            for (var ti : aTry) {
                Services.callService(ctx, "WorkflowTaskService", "execute", ti.getTask(), Void.class).await();
            }
        } catch (Exception e) {
            if (aCatch == null) {
                throw e;
            }

            if (e instanceof WorkflowErrorException we) {
                if (!matches(aCatch.getErrors() == null ? null : aCatch.getErrors().getWith(), we.getError())) {
                    throw e;
                }
                String var = aCatch.getAs() == null ? "error" : aCatch.getAs();
                ctx.set(StateKey.of(var, Error.class), we.getError());
            } else {
                throw e;
            }

            if (aCatch.getDo() != null) {
                for (var ti : aCatch.getDo()) {
                    Services.callService(ctx, "WorkflowTaskService", "execute", ti.getTask(), Void.class).await();
                }
            }
        }
    }

    private static boolean matches(ErrorFilter filter, Error err) {
        if (filter == null) {
            return true;
        }
        if (filter.getType() != null) {
            var et = err.getType();
            String t = et == null ? null : et.getLiteralErrorType().toString();
            if (!filter.getType().equals(t)) {
                return false;
            }
        }
        if (filter.getStatus() != 0 && filter.getStatus() != err.getStatus()) {
            return false;
        }
        if (filter.getInstance() != null) {
            var inst = err.getInstance();
            String i = inst == null ? null : inst.getLiteralErrorInstance();
            if (!filter.getInstance().equals(i)) {
                return false;
            }
        }
        if (filter.getTitle() != null) {
            var tt = err.getTitle();
            String t = tt == null ? null : tt.getLiteralErrorTitle();
            if (!filter.getTitle().equals(t)) {
                return false;
            }
        }
        if (filter.getDetails() != null) {
            var dt = err.getDetail();
            String d = dt == null ? null : dt.getLiteralErrorDetails();
            if (!filter.getDetails().equals(d)) {
                return false;
            }
        }
        return true;
    }
}
