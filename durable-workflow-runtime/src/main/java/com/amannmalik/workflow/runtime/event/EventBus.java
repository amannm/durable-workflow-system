package com.amannmalik.workflow.runtime.event;

import dev.restate.sdk.Awakeable;
import dev.restate.sdk.HandlerRunner;
import dev.restate.sdk.HandlerRunner.Options;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.HandlerDefinition;
import dev.restate.sdk.endpoint.definition.HandlerType;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import dev.restate.sdk.endpoint.definition.ServiceType;
import dev.restate.serde.Serde;
import dev.restate.serde.TypeRef;
import dev.restate.serde.TypeTag;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.serde.jackson.JacksonSerdes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Simple in-memory event bus implemented as a Restate virtual object. Each event type is stored
 * under a separate virtual object key.
 */
public class EventBus {

    private static final StateKey<Deque<String>> QUEUE = StateKey.of("queue", new TypeRef<>() {
    });
    private static final StateKey<List<String>> WAITERS = StateKey.of("waiters", new TypeRef<>() {
    });
    public static final ServiceDefinition DEFINITION =
            ServiceDefinition.of(
                    "EventBus",
                    ServiceType.VIRTUAL_OBJECT,
                    List.of(
                            HandlerDefinition.of(
                                    "emit",
                                    HandlerType.EXCLUSIVE,
                                    JacksonSerdes.of(String.class),
                                    Serde.VOID,
                                    HandlerRunner.of(
                                            EventBus::emit, JacksonSerdeFactory.DEFAULT, Options.DEFAULT)),
                            HandlerDefinition.of(
                                    "await",
                                    HandlerType.EXCLUSIVE,
                                    Serde.VOID,
                                    JacksonSerdes.of(String.class),
                                    HandlerRunner.of(
                                            EventBus::await,
                                            JacksonSerdeFactory.DEFAULT,
                                            Options.DEFAULT))));

    /**
     * Emit an event payload to this bus.
     */
    public static void emit(ObjectContext ctx, String payload) {
        Deque<String> queue = ctx.get(QUEUE).orElseGet(ArrayDeque::new);
        List<String> waiters = ctx.get(WAITERS).orElseGet(ArrayList::new);
        if (!waiters.isEmpty()) {
            String waiterId = waiters.removeFirst();
            ctx.awakeableHandle(waiterId).resolve(String.class, payload);
        } else {
            queue.addLast(payload);
        }
        ctx.set(QUEUE, queue);
        ctx.set(WAITERS, waiters);
    }

    /**
     * Await the next event payload on this bus.
     */
    public static String await(ObjectContext ctx) {
        Deque<String> queue = ctx.get(QUEUE).orElseGet(ArrayDeque::new);
        if (!queue.isEmpty()) {
            String payload = queue.removeFirst();
            ctx.set(QUEUE, queue);
            return payload;
        }
        Awakeable<String> awakeable = ctx.awakeable(TypeTag.of(String.class));
        List<String> waiters = ctx.get(WAITERS).orElseGet(ArrayList::new);
        waiters.add(awakeable.id());
        ctx.set(WAITERS, waiters);
        return awakeable.await();
    }
}
