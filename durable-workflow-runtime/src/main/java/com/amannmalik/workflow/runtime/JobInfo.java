package com.amannmalik.workflow.runtime;

public record JobInfo(JobRequest request, String nextExecutionTime, String nextExecutionId) {
}
