package com.amannmalik.workflow.runtime.task.run.handler;

import dev.restate.sdk.WorkflowContext;
import io.serverlessworkflow.api.types.RunShell;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShellRunHandler implements RunHandler<RunShell> {
  private static final Logger log = LoggerFactory.getLogger(ShellRunHandler.class);

  @Override
  public void handle(WorkflowContext ctx, RunShell r) {
    if (r.getShell() != null && r.getShell().getCommand() != null) {
      try {
        List<String> cmd = new java.util.ArrayList<>();
        cmd.add(r.getShell().getCommand());
        if (r.getShell().getArguments() != null) {
          r.getShell()
              .getArguments()
              .getAdditionalProperties()
              .values()
              .forEach(v -> cmd.add(v.toString()));
        }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (r.getShell().getEnvironment() != null) {
          r.getShell()
              .getEnvironment()
              .getAdditionalProperties()
              .forEach((k, v) -> pb.environment().put(k, v.toString()));
        }
        pb.start().waitFor();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
