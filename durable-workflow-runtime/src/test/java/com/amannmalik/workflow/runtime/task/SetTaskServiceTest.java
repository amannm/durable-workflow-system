package com.amannmalik.workflow.runtime.task;

import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.Set;
import io.serverlessworkflow.api.types.SetTask;
import io.serverlessworkflow.api.types.SetTaskConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SetTaskServiceTest {
    @Test
    void setsVariousTypes() {
        FakeContext ctx = new FakeContext();
        SetTask st = new SetTask();
        Set s = new Set();
        s.setString("foo");
        st.setSet(s);
        SetTaskService.execute(ctx, st);
        assertEquals("foo", ctx.get(StateKey.of("foo", String.class)).orElseThrow());
    }

    @Test
    void setsMapValues() {
        FakeContext ctx = new FakeContext();
        SetTask st = new SetTask();
        Set s = new Set();
        SetTaskConfiguration cfg = new SetTaskConfiguration();
        cfg.setAdditionalProperty("a", "b");
        cfg.setAdditionalProperty("n", 1);
        s.setSetTaskConfiguration(cfg);
        st.setSet(s);
        SetTaskService.execute(ctx, st);
        assertEquals("b", ctx.get(StateKey.of("a", String.class)).orElseThrow());
        assertEquals(1, ctx.get(StateKey.of("n", Integer.class)).orElseThrow());
    }

    static class FakeContext extends FakeWorkflowContext {
    }
}
