package com.amannmalik.workflow.runtime.cron;

import com.amannmalik.workflow.runtime.testutil.FakeWorkflowContext;
import com.amannmalik.workflow.runtime.testutil.SimpleInvocationHandle;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.common.TerminalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CronJobTest {

    FakeContext ctx;
    CronJobRequest request;

    @BeforeEach
    void setup() {
        ctx = new FakeContext();
        request =
                new CronJobRequest(
                        "* * * * *", "S", "M", Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Test
    void initiateSchedulesJob() {
        CronJobInfo info = CronJob.initiate(ctx, request);
        assertNotNull(info.nextExecutionId());
        assertTrue(ctx.get(StateKey.of("job-state", CronJobInfo.class)).isPresent());
        assertFalse(ctx.delays.isEmpty());
    }

    @Test
    void initiatingTwiceFails() {
        CronJob.initiate(ctx, request);
        assertThrows(TerminalException.class, () -> CronJob.initiate(ctx, request));
    }

    @Test
    void cancelClearsState() {
        CronJobInfo info = CronJob.initiate(ctx, request);
        CronJob.cancel(ctx);
        assertTrue(ctx.state.isEmpty());
        SimpleInvocationHandle<?> handle = ctx.handles.get(info.nextExecutionId());
        assertTrue(handle.cancelled);
    }

    @Test
    void executeReschedulesJob() {
        CronJobInfo info1 = CronJob.initiate(ctx, request);
        String firstId = info1.nextExecutionId();
        CronJob.execute(ctx);
        CronJobInfo info2 = ctx.get(StateKey.of("job-state", CronJobInfo.class)).orElseThrow();
        assertNotEquals(firstId, info2.nextExecutionId());
        assertEquals(2, ctx.delays.size());
    }

    @Test
    void invalidCronExpressionThrows() {
        CronJobRequest bad =
                new CronJobRequest("nope", "S", "M", Optional.empty(), Optional.empty(), Optional.empty());
        assertThrows(TerminalException.class, () -> CronJob.initiate(ctx, bad));
    }

    static class FakeContext extends FakeWorkflowContext {
        FakeContext() {
            super("job");
        }
    }
}
