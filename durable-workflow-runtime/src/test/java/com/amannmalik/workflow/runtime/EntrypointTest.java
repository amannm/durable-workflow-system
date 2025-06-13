package com.amannmalik.workflow.runtime;

import io.serverlessworkflow.api.types.WaitTask;
import io.serverlessworkflow.api.types.Task;
import io.serverlessworkflow.api.types.TaskItem;
import io.serverlessworkflow.api.types.Workflow;
import io.serverlessworkflow.api.types.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import dev.restate.sdk.WorkflowContext;
import java.time.Duration;
import java.util.List;

class EntrypointTest {
    @AfterEach
    void shutdown() {
    }


    @Test
    void testSetAndWaitTasks() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        Mockito.doNothing().when(ctx).sleep(Mockito.any());

        WaitTask wt = new WaitTask();
        var to = new io.serverlessworkflow.api.types.TimeoutAfter();
        var di = new io.serverlessworkflow.api.types.DurationInline();
        di.setSeconds(1);
        to.setDurationInline(di);
        wt.setWait(to);

        Task task = new Task();
        task.setWaitTask(wt);

        var wf = new Workflow(new Document(), List.of(new TaskItem("w", task)));

        Entrypoint ep = new Entrypoint();
        ep.run(ctx, wf);

        Mockito.verify(ctx).sleep(Duration.ofSeconds(1));
    }
}
