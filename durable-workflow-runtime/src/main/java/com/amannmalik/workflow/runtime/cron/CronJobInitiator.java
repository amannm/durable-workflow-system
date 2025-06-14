package com.amannmalik.workflow.runtime.cron;

import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Name;
import dev.restate.sdk.annotation.Service;

@Name("CronJobInitiator")
@Service
public class CronJobInitiator {
    @Handler
    public String create(Context ctx, CronJobRequest request) {
        var jobId = ctx.random().nextUUID().toString();
        Services.callVirtualObject(ctx, "CronJob", jobId, "initiate", request, CronJobInfo.class).await();
        return jobId;
    }
}
