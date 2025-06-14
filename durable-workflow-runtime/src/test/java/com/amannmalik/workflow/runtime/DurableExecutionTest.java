package com.amannmalik.workflow.runtime;

import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.testing.RestateRunner;
import org.junit.jupiter.api.Test;

class DurableExecutionTest {

    @Test
    void testSomething() {
        // TODO: figure out how to use this
        var endpoint = Endpoint.builder().build();
        var runner = RestateRunner.from(endpoint);
        RestateRunner build = runner.build();
        build.start();
        build.stop();
    }
}
