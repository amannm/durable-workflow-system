package com.amannmalik.workflow.runtime;

import dev.restate.common.Request;
import dev.restate.common.Target;
import dev.restate.sdk.CallDurableFuture;
import dev.restate.sdk.Context;
import dev.restate.sdk.InvocationHandle;
import dev.restate.serde.TypeRef;
import dev.restate.serde.TypeTag;

import java.time.Duration;

public class Services {

    public static <T, U> InvocationHandle<U> invokeService(Context ctx, String name, String method, T request, Class<U> responseClazz, Duration delay) {
        Target target = Target.service(name, method);
        return doSend(ctx, target, request, responseClazz, delay);
    }

    public static <T, U> InvocationHandle<U> invokeVirtualObject(Context ctx, String name, String key, String method, T request, Class<U> responseClazz, Duration delay) {
        Target target = Target.virtualObject(name, key, method);
        return doSend(ctx, target, request, responseClazz, delay);
    }

    public static <T, U> CallDurableFuture<U> callVirtualObject(Context ctx, String name, String key, String method, T request, Class<U> responseClazz) {
        Target target = Target.virtualObject(name, key, method);
        return doCall(ctx, target, request, responseClazz);
    }

    public static <T, U> CallDurableFuture<U> callService(Context ctx, String name, String method, T request, Class<U> responseClazz) {
        Target target = Target.service(name, method);
        return doCall(ctx, target, request, responseClazz);
    }

    public static <T, U> CallDurableFuture<U> callWorkflow(Context ctx, String name, String key, String method, T request, Class<U> responseClazz) {
        Target target = Target.workflow(name, key, method);
        return doCall(ctx, target, request, responseClazz);
    }

    private static <T, U> CallDurableFuture<U> doCall(Context ctx, Target target, T request, Class<U> responseClazz) {
        TypeRef<T> typeRef = new TypeRef<>() {
        };
        return ctx.call(Request.of(target, typeRef, TypeTag.of(responseClazz), request));
    }

    private static <T, U> InvocationHandle<U> doSend(Context ctx, Target target, T request, Class<U> responseClazz, Duration delay) {
        TypeRef<T> typeRef = new TypeRef<>() {
        };
        return ctx.send(Request.of(target, typeRef, TypeTag.of(responseClazz), request), delay);
    }
}
