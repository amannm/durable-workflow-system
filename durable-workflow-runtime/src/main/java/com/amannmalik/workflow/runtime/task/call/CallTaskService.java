package com.amannmalik.workflow.runtime.task.call;

import com.amannmalik.workflow.runtime.DefinitionHelper;
import com.amannmalik.workflow.runtime.task.call.handler.AsyncApiCallHandler;
import com.amannmalik.workflow.runtime.task.call.handler.CallHandler;
import com.amannmalik.workflow.runtime.task.call.handler.FunctionCallHandler;
import com.amannmalik.workflow.runtime.task.call.handler.GrpcCallHandler;
import com.amannmalik.workflow.runtime.task.call.handler.HttpCallHandler;
import com.amannmalik.workflow.runtime.task.call.handler.OpenApiCallHandler;
import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.sdk.endpoint.definition.ServiceDefinition;
import io.serverlessworkflow.api.types.CallAsyncAPI;
import io.serverlessworkflow.api.types.CallFunction;
import io.serverlessworkflow.api.types.CallGRPC;
import io.serverlessworkflow.api.types.CallHTTP;
import io.serverlessworkflow.api.types.CallOpenAPI;
import io.serverlessworkflow.api.types.CallTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CallTaskService {

    public static final StateKey<Object> RESULT = StateKey.of("call-result", Object.class);
    public static final StateKey<Map<String, io.serverlessworkflow.api.types.Task>> FUNCTIONS =
            StateKey.of("workflow-functions", new dev.restate.serde.TypeRef<>() {});
    private static final Logger log = LoggerFactory.getLogger(CallTaskService.class);
    private static final Map<Class<?>, CallHandler<?>> HANDLERS =
            Map.of(
                    CallFunction.class, new FunctionCallHandler(RESULT),
                    CallAsyncAPI.class, new AsyncApiCallHandler(RESULT),
                    CallHTTP.class, new HttpCallHandler(RESULT),
                    CallGRPC.class, new GrpcCallHandler(RESULT),
                    CallOpenAPI.class, new OpenApiCallHandler(RESULT));
    public static final ServiceDefinition DEFINITION =
            DefinitionHelper.taskService(CallTaskService.class, CallTask.class, CallTaskService::execute);

    @SuppressWarnings("unchecked")
    public static void execute(WorkflowContext ctx, CallTask task) {
        Object obj = task.get();
        CallHandler<Object> handler = (CallHandler<Object>) HANDLERS.get(obj.getClass());
        if (handler != null) {
            handler.handle(ctx, obj);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
