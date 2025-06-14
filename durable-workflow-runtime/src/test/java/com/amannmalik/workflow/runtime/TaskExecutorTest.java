package com.amannmalik.workflow.runtime;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.common.StateKey;
import io.serverlessworkflow.api.types.*;
import io.serverlessworkflow.api.types.Document;
import io.serverlessworkflow.api.types.RunTask;
import io.serverlessworkflow.api.types.RunTaskConfigurationUnion;
import io.serverlessworkflow.api.types.RunWorkflow;
import io.serverlessworkflow.api.types.SubflowConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import dev.restate.serde.TypeTag;
import com.amannmalik.workflow.runtime.WorkflowRegistry;
import java.util.Collections;

class TaskExecutorTest {

    @Test
    void executeWaitTask() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        WaitTask wt = new WaitTask();
        TimeoutAfter to = new TimeoutAfter();
        DurationInline di = new DurationInline();
        di.setSeconds(1);
        to.setDurationInline(di);
        wt.setWait(to);
        Task t = new Task();
        t.setWaitTask(wt);
        TaskExecutor.execute(ctx, t);
        Mockito.verify(ctx).sleep(Duration.ofSeconds(1));
    }

    @Test
    void executeWaitTaskIso8601() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        WaitTask wt = new WaitTask();
        TimeoutAfter to = new TimeoutAfter();
        to.setDurationExpression("PT2S");
        wt.setWait(to);
        Task t = new Task();
        t.setWaitTask(wt);
        TaskExecutor.execute(ctx, t);
        Mockito.verify(ctx).sleep(Duration.ofSeconds(2));
    }

    @Test
    void executeRunShell() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        RunTask rt = new RunTask();
        RunShell rs = new RunShell();
        Shell sh = new Shell();
        sh.setCommand("true");
        rs.setShell(sh);
        RunTaskConfigurationUnion u = new RunTaskConfigurationUnion();
        java.lang.reflect.Field f = RunTaskConfigurationUnion.class.getDeclaredFields()[0];
        f.setAccessible(true);
        try {
            f.set(u, rs);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        rt.setRun(u);
        Task t = new Task();
        t.setRunTask(rt);
        TaskExecutor.execute(ctx, t);
    }

    @Test
    void executeHttpCall() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        WireMockServer server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(WireMock.post(WireMock.urlPathEqualTo("/test"))
                .withQueryParam("q", WireMock.equalTo("1"))
                .withHeader("X-Test", WireMock.equalTo("foo"))
                .withRequestBody(WireMock.equalTo("hello"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        CallTask ct = new CallTask();
        CallHTTP ch = new CallHTTP();
        HTTPArguments args = new HTTPArguments();
        args.setMethod("POST");
        Endpoint ep = new Endpoint();
        java.lang.reflect.Field fv = Endpoint.class.getDeclaredField("value");
        fv.setAccessible(true);
        fv.set(ep, URI.create("http://localhost:" + server.port() + "/test"));
        args.setEndpoint(ep);
        HTTPHeaders hh = new HTTPHeaders();
        hh.setAdditionalProperty("X-Test", "foo");
        Headers headers = new Headers();
        headers.setHTTPHeaders(hh);
        args.setHeaders(headers);
        HTTPQuery hq = new HTTPQuery();
        hq.setAdditionalProperty("q", "1");
        Query q = new Query();
        q.setHTTPQuery(hq);
        args.setQuery(q);
        args.setBody("hello");
        ch.setWith(args);
        ct.setCallHTTP(ch);
        Task t = new Task();
        t.setCallTask(ct);
        TaskExecutor.execute(ctx, t);

        server.verify(WireMock.postRequestedFor(WireMock.urlPathEqualTo("/test"))
                .withQueryParam("q", WireMock.equalTo("1"))
                .withHeader("X-Test", WireMock.equalTo("foo"))
                .withRequestBody(WireMock.equalTo("hello")));
        server.stop();
    }

    @Test
    void executeHttpCallRedirect() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        WireMockServer server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(WireMock.get(WireMock.urlPathEqualTo("/start"))
                .willReturn(WireMock.temporaryRedirect("/dest")));
        server.stubFor(WireMock.get(WireMock.urlPathEqualTo("/dest"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        CallTask ct = new CallTask();
        CallHTTP ch = new CallHTTP();
        HTTPArguments args = new HTTPArguments();
        args.setMethod("GET");
        Endpoint ep = new Endpoint();
        java.lang.reflect.Field fv = Endpoint.class.getDeclaredField("value");
        fv.setAccessible(true);
        fv.set(ep, URI.create("http://localhost:" + server.port() + "/start"));
        args.setEndpoint(ep);
        args.setRedirect(true);
        ch.setWith(args);
        ct.setCallHTTP(ch);
        Task t = new Task();
        t.setCallTask(ct);
        TaskExecutor.execute(ctx, t);

        server.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/dest")));
        server.stop();
    }

    @Test
    void executeHttpCallEndpointInterpolation() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        Mockito.when(ctx.get(Mockito.argThat(k -> k.name().equals("petId"))))
                .thenReturn(java.util.Optional.of("42"));
        WireMockServer server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        server.start();
        server.stubFor(WireMock.get(WireMock.urlPathEqualTo("/pet/42"))
                .willReturn(WireMock.aResponse().withStatus(200)));

        CallTask ct = new CallTask();
        CallHTTP ch = new CallHTTP();
        HTTPArguments args = new HTTPArguments();
        args.setMethod("GET");
        Endpoint ep = new Endpoint();
        java.lang.reflect.Field fv = Endpoint.class.getDeclaredField("value");
        fv.setAccessible(true);
        fv.set(ep, "http://localhost:" + server.port() + "/pet/{petId}");
        args.setEndpoint(ep);
        ch.setWith(args);
        ct.setCallHTTP(ch);
        Task t = new Task();
        t.setCallTask(ct);
        TaskExecutor.execute(ctx, t);

        server.verify(WireMock.getRequestedFor(WireMock.urlPathEqualTo("/pet/42")));
        server.stop();
    }

    @Test
    void executeForkTask() {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        dev.restate.sdk.DurableFuture<Void> df = Mockito.mock(dev.restate.sdk.DurableFuture.class);
        Mockito.doReturn(null).when(df).await();
        Mockito.when(ctx.runAsync(Mockito.anyString(), Mockito.<TypeTag<Void>>any(), Mockito.any(), Mockito.any()))
                .thenAnswer(inv -> {
                    dev.restate.common.function.ThrowingSupplier<?> supplier = inv.getArgument(3);
                    try {
                        supplier.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return df;
                });
        ForkTask ft = new ForkTask();
        ForkTaskConfiguration fc = new ForkTaskConfiguration();
        WaitTask wt1 = new WaitTask();
        TimeoutAfter to1 = new TimeoutAfter();
        DurationInline di1 = new DurationInline();
        di1.setMilliseconds(10);
        to1.setDurationInline(di1);
        wt1.setWait(to1);
        Task task1 = new Task();
        task1.setWaitTask(wt1);
        TaskItem i1 = new TaskItem("b1", task1);
        TaskItem i2;
        WaitTask wt2 = new WaitTask();
        TimeoutAfter to2 = new TimeoutAfter();
        DurationInline di2 = new DurationInline();
        di2.setMilliseconds(20);
        to2.setDurationInline(di2);
        wt2.setWait(to2);
        Task task2 = new Task();
        task2.setWaitTask(wt2);
        i2 = new TaskItem("b2", task2);
        fc.setBranches(List.of(i1, i2));
        ft.setFork(fc);
        Task t = new Task();
        t.setForkTask(ft);
        TaskExecutor.execute(ctx, t);
        Mockito.verify(ctx, Mockito.times(2)).runAsync(Mockito.anyString(), Mockito.<TypeTag<Void>>any(), Mockito.any(), Mockito.any());
    }

    @Test
    void executeTryCatch() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        Mockito.doThrow(new RuntimeException()).when(ctx).sleep(Duration.ofSeconds(1));

        WaitTask wt = new WaitTask();
        TimeoutAfter to = new TimeoutAfter();
        DurationInline di = new DurationInline();
        di.setSeconds(1);
        to.setDurationInline(di);
        wt.setWait(to);
        Task waitTask = new Task();
        waitTask.setWaitTask(wt);
        TaskItem tryItem = new TaskItem("try", waitTask);

        SetTask st = new SetTask();
        Set set = new Set();
        java.lang.reflect.Field fs = Set.class.getDeclaredField("string");
        fs.setAccessible(true);
        fs.set(set, "handled");
        java.lang.reflect.Field fv = Set.class.getDeclaredField("value");
        fv.setAccessible(true);
        fv.set(set, "yes");
        st.setSet(set);
        Task setTask = new Task();
        setTask.setSetTask(st);
        TaskItem catchItem = new TaskItem("catch", setTask);

        TryTaskCatch tc = new TryTaskCatch();
        tc.setDo(List.of(catchItem));
        TryTask tt = new TryTask();
        tt.setTry(List.of(tryItem));
        tt.setCatch(tc);
        Task t = new Task();
        t.setTryTask(tt);
        TaskExecutor.execute(ctx, t);
        Mockito.verify(ctx).set(Mockito.argThat(k -> k.name().equals("handled")), Mockito.eq("yes"));
    }

    @Test
    void executeSetTask() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);
        SetTask st = new SetTask();
        Set s = new Set();
        java.lang.reflect.Field fString = Set.class.getDeclaredField("string");
        fString.setAccessible(true);
        fString.set(s, "foo");
        java.lang.reflect.Field fValue = Set.class.getDeclaredField("value");
        fValue.setAccessible(true);
        fValue.set(s, "bar");
        st.setSet(s);
        Task t = new Task();
        t.setSetTask(st);
        TaskExecutor.execute(ctx, t);
        org.mockito.ArgumentCaptor<StateKey> k = org.mockito.ArgumentCaptor.forClass(StateKey.class);
        org.mockito.ArgumentCaptor<String> v = org.mockito.ArgumentCaptor.forClass(String.class);
        Mockito.verify(ctx).set(k.capture(), v.capture());
        assertEquals("foo", k.getValue().name());
        assertEquals("bar", v.getValue());
    }

    @Test
    void executeRunWorkflow() throws Exception {
        WorkflowContext ctx = Mockito.mock(WorkflowContext.class);

        // Define sub workflow that sets a flag
        Workflow sub = new Workflow();
        sub.setDocument(new Document("1.0.0", "test", "sub", "0.1.0"));
        SetTask st = new SetTask();
        Set set = new Set();
        java.lang.reflect.Field fs = Set.class.getDeclaredField("string");
        fs.setAccessible(true);
        fs.set(set, "flag");
        java.lang.reflect.Field fv = Set.class.getDeclaredField("value");
        fv.setAccessible(true);
        fv.set(set, "yes");
        st.setSet(set);
        Task subTask = new Task();
        subTask.setSetTask(st);
        sub.setDo(Collections.singletonList(new TaskItem("set", subTask)));

        WorkflowRegistry.register(sub);

        // Root workflow running the sub workflow
        Workflow root = new Workflow();
        root.setDocument(new Document("1.0.0", "test", "root", "0.1.0"));
        RunWorkflow rw = new RunWorkflow();
        SubflowConfiguration cfg = new SubflowConfiguration();
        cfg.setNamespace("test");
        cfg.setName("sub");
        cfg.setVersion("0.1.0");
        rw.setWorkflow(cfg);
        RunTaskConfigurationUnion u = new RunTaskConfigurationUnion();
        java.lang.reflect.Field fu = RunTaskConfigurationUnion.class.getDeclaredFields()[0];
        fu.setAccessible(true);
        fu.set(u, rw);
        RunTask rt = new RunTask();
        rt.setRun(u);
        Task t = new Task();
        t.setRunTask(rt);
        root.setDo(Collections.singletonList(new TaskItem("run", t)));

        Entrypoint ep = new Entrypoint();
        ep.run(ctx, root);

        Mockito.verify(ctx).set(Mockito.argThat(k -> k.name().equals("flag")), Mockito.eq("yes"));

        WorkflowRegistry.clear();
    }
}
