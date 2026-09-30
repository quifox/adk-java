/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.adk.tools.mcp;

import static com.google.common.truth.Truth.assertThat;

import io.modelcontextprotocol.client.McpAsyncClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.ServerParameters;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCRequest;
import io.modelcontextprotocol.spec.ProtocolVersions;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;

/** Offline construction tests for MCP stdio parameter handling. */
@RunWith(JUnit4.class)
public final class McpSessionManagerTest {

  @Test
  public void initializeAsyncSession_stdioWithDefaultBuilder_buildsClient() {
    StdioConnectionParameters connectionParams = stdioConnectionParameters();
    assertThat(connectionParams.timeoutDuration()).isEqualTo(Duration.ofSeconds(5));

    McpAsyncClient client = McpSessionManager.initializeAsyncSession(connectionParams);
    try {
      assertThat(client).isNotNull();
    } finally {
      // The stdio transport launches a process only when connected.
      client.closeGracefully().block(Duration.ofSeconds(1));
    }
  }

  @Test
  public void initializeAsyncSession_stdioPassesSdkParametersToTransportBuilder() {
    AtomicReference<Object> receivedParameters = new AtomicReference<>();
    McpAsyncClient client =
        McpSessionManager.initializeAsyncSession(
            stdioConnectionParameters(17f),
            parameters -> {
              receivedParameters.set(parameters);
              return new TestTransport();
            });
    try {
      assertThat(receivedParameters.get()).isInstanceOf(ServerParameters.class);
      assertThat(((ServerParameters) receivedParameters.get()).getCommand())
          .isEqualTo("unused-command");
    } finally {
      client.closeGracefully().block(Duration.ofSeconds(1));
    }
  }

  @Test
  public void initializeAsyncSession_stdioAppliesConfiguredRequestTimeout() throws Exception {
    McpAsyncClient client =
        McpSessionManager.initializeAsyncSession(
            stdioConnectionParameters(/* timeoutSeconds= */ 0.5f), unused -> new TestTransport());
    try {
      // The fake transport completes initialization locally, then leaves ping unanswered. A short
      // configured timeout must therefore expire the SDK request without a real server or a long
      // sleep. Capture the asynchronous terminal signal directly so a Mono.block timeout cannot
      // masquerade as the SDK request timeout under test.
      client.initialize().block(Duration.ofSeconds(3));
      CountDownLatch pingTerminated = new CountDownLatch(1);
      AtomicReference<Throwable> pingFailure = new AtomicReference<>();
      AtomicBoolean pingCompleted = new AtomicBoolean();
      client
          .ping()
          .subscribe(
              unused -> {},
              failure -> {
                pingFailure.set(failure);
                pingTerminated.countDown();
              },
              () -> {
                pingCompleted.set(true);
                pingTerminated.countDown();
              });

      assertThat(pingTerminated.await(3, TimeUnit.SECONDS)).isTrue();
      assertThat(pingCompleted.get()).isFalse();
      assertThat(Exceptions.unwrap(pingFailure.get())).isInstanceOf(TimeoutException.class);
    } finally {
      client.closeGracefully().block(Duration.ofSeconds(1));
    }
  }

  @Test
  public void initializeAsyncSession_sseNullTimeoutsUseDefaults() {
    SseServerParameters connectionParams =
        SseServerParameters.builder()
            .url("http://localhost:1234")
            .timeout(null)
            .sseReadTimeout(null)
            .build();
    AtomicReference<Object> receivedParameters = new AtomicReference<>();
    McpAsyncClient client =
        McpSessionManager.initializeAsyncSession(
            connectionParams,
            parameters -> {
              receivedParameters.set(parameters);
              return new TestTransport();
            });
    try {
      assertThat(receivedParameters.get()).isSameInstanceAs(connectionParams);
      assertThat(client.initialize().block(Duration.ofSeconds(1))).isNotNull();
    } finally {
      client.closeGracefully().block(Duration.ofSeconds(1));
    }
  }

  @Test
  public void initializeSession_stdioUsesSdkParametersAndStillInitializes() {
    AtomicReference<Object> receivedParameters = new AtomicReference<>();
    McpSyncClient client =
        McpSessionManager.initializeSession(
            stdioConnectionParameters(/* timeoutSeconds= */ 1f),
            parameters -> {
              receivedParameters.set(parameters);
              return new TestTransport();
            });
    try {
      assertThat(receivedParameters.get()).isInstanceOf(ServerParameters.class);
      assertThat(client).isNotNull();
    } finally {
      client.close();
    }
  }

  private static StdioConnectionParameters stdioConnectionParameters() {
    return stdioConnectionParameters(/* timeoutSeconds= */ 5f);
  }

  private static StdioConnectionParameters stdioConnectionParameters(float timeoutSeconds) {
    return StdioConnectionParameters.builder()
        .serverParams(StdioServerParameters.builder().command("unused-command").build())
        .timeout(timeoutSeconds)
        .build();
  }

  private static final class TestTransport implements McpClientTransport {
    private Function<Mono<JSONRPCMessage>, Mono<JSONRPCMessage>> inboundHandler;

    @Override
    public Mono<Void> connect(Function<Mono<JSONRPCMessage>, Mono<JSONRPCMessage>> handler) {
      this.inboundHandler = handler;
      return Mono.empty();
    }

    @Override
    public Mono<Void> closeGracefully() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> sendMessage(JSONRPCMessage message) {
      if (message instanceof JSONRPCRequest request
          && request.method().equals(McpSchema.METHOD_INITIALIZE)) {
        return Mono.fromRunnable(
            () ->
                this.inboundHandler
                    .apply(
                        Mono.just(
                            McpSchema.JSONRPCResponse.result(request.id(), initializeResult())))
                    .subscribe());
      }
      return Mono.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unmarshalFrom(Object data, TypeRef<T> typeRef) {
      return (T) data;
    }

    private static McpSchema.InitializeResult initializeResult() {
      return McpSchema.InitializeResult.builder(
              ProtocolVersions.MCP_2025_11_25,
              McpSchema.ServerCapabilities.builder().build(),
              McpSchema.Implementation.builder("test-server", "1").build())
          .build();
    }
  }
}
