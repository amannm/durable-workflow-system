package com.amannmalik.workflow.runtime;

import com.amannmalik.workflow.runtime.task.WorkflowTaskService;
import dev.restate.client.Client;
import dev.restate.common.RequestBuilder;
import dev.restate.sdk.testing.BindService;
import dev.restate.sdk.testing.RestateClient;
import dev.restate.sdk.testing.RestateTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@RestateTest
class DurableExecutionTest {

    @BindService
    WorkflowTaskService service = new WorkflowTaskService();

    // TODO: tests

    @Test
    void testSomething(@RestateClient Client client) {
        var response = client.call(RequestBuilder);
        Assertions.assertEquals(response, "Hello, Francesco!");
    }
}
