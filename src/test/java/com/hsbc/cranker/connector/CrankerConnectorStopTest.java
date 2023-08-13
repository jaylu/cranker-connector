package com.hsbc.cranker.connector;


import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.ContentTypes;
import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsbc.cranker.connector.BaseEndToEndTest.registrationUri;
import static com.hsbc.cranker.connector.BaseEndToEndTest.startConnectorAndWaitForRegistration;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;

public class CrankerConnectorStopTest {

    private static final Logger log = LoggerFactory.getLogger(CrankerConnectorStopTest.class);

    private final HttpClient httpClient = HttpUtils.createHttpClientBuilder(true)
            .version(HttpClient.Version.HTTP_2)
            .build();
    private CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer routerServer;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
        if (targetServer != null) swallowException(targetServer::stop);
        if (routerServer != null) swallowException(routerServer::stop);
        if (crankerRouter != null) swallowException(crankerRouter::stop);
    }

    @Test
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout() throws ExecutionException, InterruptedException, TimeoutException {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
                .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                    serverCounter.incrementAndGet();
                    Thread.sleep(2000L);
                    response.status(201);
                    response.write("hello world");
                    log.info("server response completed!");
                })
                .start();

        this.crankerRouter = crankerRouter()
                .withConnectorMaxWaitInMillis(4000)
                .start();

        this.routerServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
                .addHandler(crankerRouter.createRegistrationHandler())
                .addHandler(crankerRouter.createHttpHandler())
                .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);

        int requestCount = 3;
        for (int i = 0; i < requestCount; i++) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(this.routerServer.uri().resolve("/test"))
                    .build();
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .whenComplete((response, throwable) -> {
                        log.info("client received response !");
                        assertThat(response.statusCode(), is(201));
                        assertThat(response.body(), is("hello world"));
                        clientCounter.incrementAndGet();
                        log.info("client: request completed");
                    });
        }

        assertEventually(serverCounter::get, equalTo(requestCount));
        assertEventually(clientCounter::get, lessThan(requestCount));

        log.info("connector stopping");
        long start = System.currentTimeMillis();
        boolean stop = this.connector.stop(10, TimeUnit.SECONDS);
        log.info("connector stopped, isSuccess={}, used={}", stop, System.currentTimeMillis() - start);
        assertThat(stop, is(true));
        this.targetServer.stop();

        assertEventually(clientCounter::get, equalTo(requestCount));
    }

    @Test
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout_LongQuery() throws ExecutionException, InterruptedException, TimeoutException {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
                .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                    serverCounter.incrementAndGet();
                    response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
                    try (PrintWriter writer = response.writer()) {
                        for (int i = 0; i < 12; i++) {
                            String toWrite = i + ",";
                            writer.print(toWrite);
                            writer.flush();
                            log.info("response writing {}", toWrite);
                            Thread.sleep(1000L);
                        }
                    }
                })
                .start();

        this.crankerRouter = crankerRouter()
                .withConnectorMaxWaitInMillis(4000)
                .start();

        this.routerServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
                .addHandler(crankerRouter.createRegistrationHandler())
                .addHandler(crankerRouter.createHttpHandler())
                .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);


        HttpRequest request = HttpRequest.newBuilder()
                .uri(this.routerServer.uri().resolve("/test"))
                .build();
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete((response, throwable) -> {
                    log.info("response completed");
                    assertThat(response.statusCode(), is(200));
                    assertThat(response.body(), is("0,1,2,3,4,5,6,7,8,9,10,11,"));
                    clientCounter.incrementAndGet();
                });
        assertEventually(serverCounter::get, equalTo(1));

        log.info("connector stopping");
        long start = System.currentTimeMillis();
        boolean stop = this.connector.stop(15, TimeUnit.SECONDS);
        log.info("connector stopped, isSuccess={}, used={}", stop, System.currentTimeMillis() - start);
        assertThat(stop, is(true));
        this.targetServer.stop();

        assertEventually(clientCounter::get, equalTo(1));
    }


    @Test
    void getTimeoutExceptionIfExceedTimeout() throws ExecutionException, InterruptedException, TimeoutException {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger serverExceptionCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
                .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                    serverCounter.incrementAndGet();
                    Thread.sleep(10000L);
                    response.status(201);
                    response.write("hello world");
                    log.info("server response complete!");
                })
                .start();

        this.crankerRouter = crankerRouter()
                .withConnectorMaxWaitInMillis(4000)
                .start();

        this.routerServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
                .addHandler(crankerRouter.createRegistrationHandler())
                .addHandler(crankerRouter.createHttpHandler())
                .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);

        int requestCount = 3;
        for (int i = 0; i < requestCount; i++) {
            httpClient.sendAsync(HttpRequest.newBuilder()
                            .uri(this.routerServer.uri().resolve("/test"))
                            .build(), HttpResponse.BodyHandlers.ofString())
                    .whenComplete((response, throwable) -> {
                        log.info("client received response!");
                        assertThat(response.statusCode(), is(201));
                        assertThat(response.body(), is("hello world"));
                        clientCounter.incrementAndGet();
                        log.info("client: request completed");
                    });
        }

        assertEventually(serverCounter::get, equalTo(requestCount), 5, 200);
        assertEventually(clientCounter::get, lessThan(requestCount), 5, 200);


        assertThat(this.connector.stop(1, TimeUnit.SECONDS), is(false));
        this.targetServer.stop();
    }

    @Test
    public void returnFalseWhenCallingStopBeforeCallingStart() {

        this.targetServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
                .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                    response.write("hello world");
                })
                .start();

        this.crankerRouter = crankerRouter()
                .withConnectorMaxWaitInMillis(4000)
                .start();

        this.routerServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
                .addHandler(crankerRouter.createRegistrationHandler())
                .addHandler(crankerRouter.createHttpHandler())
                .start();

        this.connector = CrankerConnectorBuilder.connector()
                .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
                .withRouterUris(RegistrationUriSuppliers.fixedUris(URI.create("wss://localhost:1234")))
                .withRoute("*")
                .withTarget(URI.create("https://test-url"))
                .withComponentName("cranker-connector-unit-test")
                .build(); // not start

        // call before start
        assertFalse(() -> connector.stop(1, TimeUnit.SECONDS));
    }


    @Test
    public void returnFalseWhenCallingStopMultipleTime() {

        this.targetServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
                .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                    response.write("hello world");
                })
                .start();

        this.crankerRouter = crankerRouter()
                .withConnectorMaxWaitInMillis(4000)
                .start();

        this.routerServer = httpsServer()
                .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
                .addHandler(crankerRouter.createRegistrationHandler())
                .addHandler(crankerRouter.createHttpHandler())
                .start();

        this.connector = CrankerConnectorBuilder.connector()
                .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
                .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(routerServer.uri())))
                .withRoute("*")
                .withTarget(URI.create("https://test-url"))
                .withComponentName("cranker-connector-unit-test")
                .start();

        // call stop the first time
        connector.stop(10, TimeUnit.SECONDS);

        // second time will throw exception
        assertFalse(() -> connector.stop(1, TimeUnit.SECONDS));

    }
}
