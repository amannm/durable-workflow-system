package com.amannmalik.workflow.runtime.cron;

import java.time.Instant;

public record CronJobInfo(
        CronJobRequest request, Instant nextExecutionTime, String nextExecutionId) {
}
