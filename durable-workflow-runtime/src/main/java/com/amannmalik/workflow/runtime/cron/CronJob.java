package com.amannmalik.workflow.runtime.cron;

import com.amannmalik.workflow.runtime.Services;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.fasterxml.jackson.core.type.TypeReference;
import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.HandlerRunner.Options;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.SharedObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.common.TerminalException;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.TypeTag;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;
import io.serverlessworkflow.api.types.Workflow;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static com.cronutils.model.CronType.UNIX;

public class CronJob {

    private static final StateKey<CronJobInfo> JOB_STATE =
            StateKey.of("job-state", CronJobInfo.class);
    private static final CronParser PARSER =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(UNIX));
    public static final ServiceDefinition DEFINITION =
            ServiceDefinition.of(
                    "CronJob",
                    ServiceType.VIRTUAL_OBJECT,
                    List.of(
                            HandlerDefinition.of(
                                    "initiate",
                                    HandlerType.EXCLUSIVE,
                                    JacksonSerdes.of(CronJobRequest.class),
                                    JacksonSerdes.of(CronJobInfo.class),
                                    HandlerRunner.of(
                                            CronJob::initiate,
                                            JacksonSerdeFactory.DEFAULT,
                                            Options.DEFAULT)),
                            HandlerDefinition.of(
                                    "execute",
                                    HandlerType.EXCLUSIVE,
                                    Serde.VOID,
                                    Serde.VOID,
                                    HandlerRunner.of(
                                            CronJob::execute,
                                            JacksonSerdeFactory.DEFAULT,
                                            Options.DEFAULT)),
                            HandlerDefinition.of(
                                    "cancel",
                                    HandlerType.EXCLUSIVE,
                                    Serde.VOID,
                                    Serde.VOID,
                                    HandlerRunner.of(
                                            CronJob::cancel, JacksonSerdeFactory.DEFAULT, Options.DEFAULT)),
                            HandlerDefinition.of(
                                    "getInfo",
                                    HandlerType.SHARED,
                                    Serde.VOID,
                                    JacksonSerdes.of(new TypeReference<>() {
                                    }),
                                    HandlerRunner.of(
                                            CronJob::getInfo,
                                            JacksonSerdeFactory.DEFAULT,
                                            Options.DEFAULT))));

    public static CronJobInfo initiate(ObjectContext ctx, CronJobRequest request) {
        if (ctx.get(JOB_STATE).isPresent()) {
            throw new TerminalException("Job already exists for this ID");
        }
        return scheduleNextExecution(ctx, request);
    }

    public static void execute(ObjectContext ctx) {
        CronJobRequest request =
                ctx.get(JOB_STATE).orElseThrow(() -> new TerminalException("Job not found")).request();

        executeTask(ctx, request);
        scheduleNextExecution(ctx, request);
    }

    public static void cancel(ObjectContext ctx) {
        ctx.get(JOB_STATE)
                .ifPresent(jobState -> ctx.invocationHandle(jobState.nextExecutionId()).cancel());

        // Clear the job state
        ctx.clearAll();
    }

    public static Optional<CronJobInfo> getInfo(SharedObjectContext ctx) {
        return ctx.get(JOB_STATE);
    }

    private static void executeTask(ObjectContext ctx, CronJobRequest job) {
        Target target = (job.key().isPresent())
                        ? Target.virtualObject(job.service(), job.method(), job.key().get())
                        : Target.service(job.service(), job.method());
        var request = (job.workflow().isPresent())
                        ? Request.of(
                                target, TypeTag.of(Workflow.class), TypeTag.of(Void.class), job.workflow().get())
                        : (job.payload().isPresent())
                                ? Request.of(
                                        target, TypeTag.of(String.class), TypeTag.of(Void.class), job.payload().get())
                                : Request.of(target, new byte[0]);
        ctx.send(request);
    }

    private static CronJobInfo scheduleNextExecution(ObjectContext ctx, CronJobRequest request) {
        // Parse cron expression
        ExecutionTime executionTime;
        try {
            executionTime = ExecutionTime.forCron(PARSER.parse(request.cronExpression()));
        } catch (IllegalArgumentException e) {
            throw new TerminalException("Invalid cron expression: " + e.getMessage());
        }

        // Calculate next execution time
        var now = ctx.run(ZonedDateTime.class, ZonedDateTime::now);
        var delay = executionTime
                        .timeToNextExecution(now)
                        .orElseThrow(() -> new TerminalException("Cannot determine next execution time"));
        var next = executionTime
                        .nextExecution(now)
                        .orElseThrow(() -> new TerminalException("Cannot determine next execution time"));

        // Schedule next execution for this job
        String thisJobId = ctx.key(); // This got generated by the CronJobInitiator
        var handle = Services.invokeVirtualObject(
                        ctx, "CronJob", thisJobId, "execute", request, CronJobInfo.class, delay);
        // Save job state
        var jobState = new CronJobInfo(request, next.toInstant(), handle.invocationId());
        ctx.set(JOB_STATE, jobState);
        return jobState;
    }
}
