package com.amannmalik.workflow.runtime.cron;

import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.Context;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;

import java.util.List;

public class CronJobInitiator {

    public static final ServiceDefinition DEFINITION = ServiceDefinition.of(
            "CronJobInitiator",
            ServiceType.SERVICE,
            List.of(
                    HandlerDefinition.of(
                            "create",
                            HandlerType.SHARED,
                            JacksonSerdes.of(CronJobRequest.class),
                            JacksonSerdes.of(String.class),
                            HandlerRunner.of(CronJobInitiator::create, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                    )
            )
    );

    public static String create(Context ctx, CronJobRequest request) {
        var jobId = ctx.random().nextUUID().toString();
        Services.callVirtualObject(ctx, "CronJob", jobId, "initiate", request, CronJobInfo.class).await();
        return jobId;
    }
}
