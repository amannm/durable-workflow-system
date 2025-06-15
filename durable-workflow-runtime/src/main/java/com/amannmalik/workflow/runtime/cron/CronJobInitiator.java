package com.amannmalik.workflow.runtime.cron;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.Services;
import dev.restate.sdk.Context;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;

public class CronJobInitiator {

  public static final ServiceDefinition DEFINITION =
      DefinitionHelper.singleVoidHandlerService(
          "CronJobInitiator",
          ServiceType.SERVICE,
          "create",
          HandlerType.SHARED,
          CronJobRequest.class,
          CronJobInitiator::create);

  public static String create(Context ctx, CronJobRequest request) {
    var jobId = ctx.random().nextUUID().toString();
    Services.callVirtualObject(ctx, "CronJob", jobId, "initiate", request, CronJobInfo.class)
        .await();
    return jobId;
  }
}
