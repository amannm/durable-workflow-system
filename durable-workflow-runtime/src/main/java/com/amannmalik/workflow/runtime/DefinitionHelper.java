package com.amannmalik.workflow.runtime;

import dev.restate.common.function.ThrowingBiConsumer;
import dev.restate.sdk.Context;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;

import java.util.List;

/**
 * Utility helpers for building {@link ServiceDefinition} constants.
 */
public final class DefinitionHelper {

    private DefinitionHelper() {
    }

    /**
     * Build a service definition for a task service with a single shared handler named "execute".
     */
    public static <REQ> ServiceDefinition taskService(
            Class<?> serviceClass,
            Class<REQ> requestClass,
            ThrowingBiConsumer<WorkflowContext, REQ> handler) {
        return singleVoidHandlerService(
                serviceClass,
                ServiceType.SERVICE,
                "execute",
                HandlerType.SHARED,
                requestClass,
                handler
        );
    }

    /**
     * Generic helper to create a service definition with a single void-returning handler.
     */
    public static <CTX extends Context, REQ> ServiceDefinition singleVoidHandlerService(
            Class<?> serviceClass,
            ServiceType serviceType,
            String handlerName,
            HandlerType handlerType,
            Class<REQ> requestClass,
            ThrowingBiConsumer<CTX, REQ> handler) {
        return singleVoidHandlerService(
                serviceClass.getSimpleName(),
                serviceType,
                handlerName,
                handlerType,
                requestClass,
                handler
        );
    }

    /**
     * Variant allowing a custom service name.
     */
    public static <CTX extends Context, REQ> ServiceDefinition singleVoidHandlerService(
            String serviceName,
            ServiceType serviceType,
            String handlerName,
            HandlerType handlerType,
            Class<REQ> requestClass,
            ThrowingBiConsumer<CTX, REQ> handler) {
        return ServiceDefinition.of(
                serviceName,
                serviceType,
                List.of(
                        HandlerDefinition.of(
                                handlerName,
                                handlerType,
                                JacksonSerdes.of(requestClass),
                                Serde.VOID,
                                HandlerRunner.of(handler, JacksonSerdeFactory.DEFAULT, HandlerRunner.Options.DEFAULT)
                        )
                )
        );
    }
}
