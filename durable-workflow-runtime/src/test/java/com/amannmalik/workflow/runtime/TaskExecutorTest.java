package com.amannmalik.workflow.runtime;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

class TaskExecutorTest {

    @Test
    void executeWaitTask() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        WaitTask wt = new WaitTask();
        TimeoutAfter to = new TimeoutAfter();
        DurationInline di = new DurationInline();
        di.setSeconds(1);
        to.setDurationInline(di);
        wt.setWait(to);
        Task t = new Task();
        t.setWaitTask(wt);
        TaskExecutor.execute(ctx, t);
        Mockito.verify(ctx).sleep(Duration.ofSeconds(1));
    }

    @Test
    void executeSetTask() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        SetTask st = new SetTask();
        Set s = new Set();
        java.lang.reflect.Field fString = Set.class.getDeclaredField("string");
        fString.setAccessible(true);
        fString.set(s, "foo");
        java.lang.reflect.Field fValue = Set.class.getDeclaredField("value");
        fValue.setAccessible(true);
        fValue.set(s, "bar");
        st.setSet(s);
        Task t = new Task();
        t.setSetTask(st);
        TaskExecutor.execute(ctx, t);
        org.mockito.ArgumentCaptor<StateKey> k = org.mockito.ArgumentCaptor.forClass(StateKey.class);
        org.mockito.ArgumentCaptor<String> v = org.mockito.ArgumentCaptor.forClass(String.class);
        Mockito.verify(ctx).set(k.capture(), v.capture());
        assertEquals("foo", k.getValue().name());
        assertEquals("bar", v.getValue());
    }
}
