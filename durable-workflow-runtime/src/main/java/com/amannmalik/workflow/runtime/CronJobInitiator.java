package com.amannmalik.workflow.runtime;

import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Name;
import dev.restate.sdk.annotation.Service;
import dev.restate.serde.TypeTag;

@Name("CronJobInitiator")
@Service
public class CronJobInitiator {
    @Handler
    public String create(Context ctx, JobRequest request) {
        // Create a new job ID and initiate the cron job object for that ID
        // We can then address this job object by its ID
        var jobId = ctx.random().nextUUID().toString();
        Target target = Target.virtualObject("CronJob", jobId, "initiate");
        JobInfo cronJob = ctx.call(Request.of(target, TypeTag.of(JobRequest.class), TypeTag.of(JobInfo.class), request)).await();
        return String.format(
                "Job created with ID %s and next execution time %s", jobId, cronJob.nextExecutionTime());
    }
}
