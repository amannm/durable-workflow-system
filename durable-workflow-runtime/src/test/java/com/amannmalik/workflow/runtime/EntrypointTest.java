package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.cron.CronJob;
import com.amannmalik.workflow.runtime.cron.CronJobInitiator;
import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import dev.restate.sdk.endpoint.Endpoint;
import io.serverlessworkflow.api.types.Document;
import io.serverlessworkflow.api.types.DurationInline;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.TimeoutAfter;
import io.serverlessworkflow.api.types.WaitTask;
import io.serverlessworkflow.api.types.Workflow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class EntrypointTest {
  private static Workflow createTestWorkflow() {
    WaitTask wt = new WaitTask();
    var to = new TimeoutAfter();
    var di = new DurationInline();
    di.setSeconds(1);
    to.setDurationInline(di);
    wt.setWait(to);
    Task task = new Task();
    task.setWaitTask(wt);
    return new Workflow(new Document(), List.of(new TaskItem("w", task)));
  }

  @AfterEach
  void shutdown() {
  }

  @Test
  void testSetAndWaitTasks() {
    var workflow = createTestWorkflow();
    var builder =
            Endpoint.builder()
                    .bind(WorkflowRunner.DEFINITION)
                    .bind(WorkflowTaskService.DEFINITION)
                    .bind(CronJobInitiator.DEFINITION)
                    .bind(CronJob.DEFINITION);
    var endpoint = builder.build();
    // TODO: test if it works
  }
}
