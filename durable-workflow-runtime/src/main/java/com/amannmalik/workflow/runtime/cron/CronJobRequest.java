package com.amannmalik.workflow.runtime.cron;

import io.serverlessworkflow.api.types.Workflow;
import java.util.Optional;

public record CronJobRequest(
    String cronExpression, // e.g. "0 0 * * *" (every day at midnight)
    String service,
    String method, // Handler to execute with this schedule
    Optional<String> key, // Optional Virtual Object key of the task to call
    Optional<String> payload,
    Optional<Workflow> workflow) {} // Optional data to pass to the handler or workflow to run
