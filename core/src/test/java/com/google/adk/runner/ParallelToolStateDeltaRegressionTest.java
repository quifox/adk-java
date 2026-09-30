/*
 * Copyright 2026 Google LLC
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
package com.google.adk.runner;

import static com.google.common.truth.Truth.assertThat;

import com.google.adk.agents.InvocationContext;
import com.google.adk.agents.LlmAgent;
import com.google.adk.agents.RunConfig;
import com.google.adk.agents.RunConfig.ToolExecutionMode;
import com.google.adk.apps.App;
import com.google.adk.events.Event;
import com.google.adk.flows.llmflows.Functions;
import com.google.adk.models.LlmResponse;
import com.google.adk.sessions.InMemorySessionService;
import com.google.adk.sessions.Session;
import com.google.adk.testing.TestLlm;
import com.google.adk.tools.BaseTool;
import com.google.adk.tools.ToolContext;
import com.google.genai.types.Content;
import com.google.genai.types.FunctionCall;
import com.google.genai.types.FunctionDeclaration;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.subjects.CompletableSubject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests shared-state writes through tool execution, response merging, and session persistence. */
final class ParallelToolStateDeltaRegressionTest {
  private static final String APP = "parallel_state_test";

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void outOfOrderListAppends_preserveLatestWriteAndResponseOrder(ToolExecutionMode mode) {
    CompletableSubject bWritten = CompletableSubject.create();
    AtomicReference<Object> observedByA = new AtomicReference<>();
    BaseTool a =
        tool(
            "a",
            context ->
                bWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          Object previous = context.state().get("items");
                          observedByA.set(previous);
                          List<Object> updated = new ArrayList<>((List<?>) previous);
                          updated.add("a");
                          context.state().put("items", updated);
                          return Map.of("result", "a");
                        })));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.state().put("items", List.of("b"));
                      bWritten.onComplete();
                      return Map.of("result", "b");
                    }));
    Result result = run(List.of(a, b), mode, Map.of());
    assertThat(observedByA.get()).isEqualTo(List.of("b"));
    assertThat(result.session.state().get("items")).isEqualTo(List.of("b", "a"));
    assertThat(result.response.functionResponses().stream().map(fr -> fr.id().orElse("")).toList())
        .containsExactly("a_call", "b_call")
        .inOrder();
    assertThat(result.response.actions().stateDelta().get("items")).isEqualTo(List.of("b", "a"));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void inOrderListAppends_doNotDuplicateEntries(ToolExecutionMode mode) {
    CompletableSubject aWritten = CompletableSubject.create();
    BaseTool a =
        tool(
            "a",
            context ->
                Single.fromCallable(
                    () -> {
                      context.state().put("items", List.of("a"));
                      aWritten.onComplete();
                      return Map.of("result", "a");
                    }));
    BaseTool b =
        tool(
            "b",
            context ->
                aWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          List<Object> updated =
                              new ArrayList<>((List<?>) context.state().get("items"));
                          updated.add("b");
                          context.state().put("items", updated);
                          return Map.of("result", "b");
                        })));
    assertThat(run(List.of(a, b), mode, Map.of()).session.state().get("items"))
        .isEqualTo(List.of("a", "b"));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void outOfOrderNestedMapSnapshots_preserveLatestList(ToolExecutionMode mode) {
    CompletableSubject bWritten = CompletableSubject.create();
    BaseTool a =
        tool(
            "a",
            context ->
                bWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          Map<?, ?> doc = (Map<?, ?>) context.state().get("doc");
                          List<?> entities = (List<?>) doc.get("entities");
                          Map<?, ?> entity = (Map<?, ?>) entities.get(0);
                          List<Object> labels = new ArrayList<>((List<?>) entity.get("labels"));
                          labels.add("a");
                          context
                              .state()
                              .put(
                                  "doc",
                                  Map.of(
                                      "entities", List.of(Map.of("name", "e1", "labels", labels))));
                          return Map.of("result", "a");
                        })));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context
                          .state()
                          .put(
                              "doc",
                              Map.of(
                                  "entities",
                                  List.of(Map.of("name", "e1", "labels", List.of("b")))));
                      bWritten.onComplete();
                      return Map.of("result", "b");
                    }));
    assertThat(run(List.of(a, b), mode, Map.of()).session.state().get("doc"))
        .isEqualTo(Map.of("entities", List.of(Map.of("name", "e1", "labels", List.of("b", "a")))));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void separateMapKeys_areMergedRatherThanReplacedByLatestSnapshot(ToolExecutionMode mode) {
    CompletableSubject bWritten = CompletableSubject.create();
    BaseTool a =
        tool(
            "a",
            context ->
                bWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          context.state().put("drafts", Map.of("a", "body"));
                          return Map.of("result", "a");
                        })));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.state().put("drafts", Map.of("b", "body"));
                      bWritten.onComplete();
                      return Map.of("result", "b");
                    }));
    assertThat(run(List.of(a, b), mode, Map.of()).session.state().get("drafts"))
        .isEqualTo(Map.of("a", "body", "b", "body"));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void directListDeltas_keepCallOrderWithoutRestoringUnrelatedSessionValue(ToolExecutionMode mode) {
    CompletableSubject bWritten = CompletableSubject.create();
    BaseTool a =
        tool(
            "a",
            context ->
                bWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          context.eventActions().stateDelta().put("items", List.of("a"));
                          return Map.of("result", "a");
                        })));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.eventActions().stateDelta().put("items", List.of("b"));
                      bWritten.onComplete();
                      return Map.of("result", "b");
                    }));
    assertThat(
            run(List.of(a, b), mode, Map.of("items", List.of("old"))).session.state().get("items"))
        .isEqualTo(List.of("b"));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void equalButDistinctDirectDelta_doesNotEstablishLatestWriteIdentity(ToolExecutionMode mode) {
    BaseTool a =
        tool(
            "a",
            context ->
                Single.fromCallable(
                    () -> {
                      context
                          .eventActions()
                          .stateDelta()
                          .put("items", new ArrayList<>(List.of("old")));
                      return Map.of("result", "a");
                    }));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.eventActions().stateDelta().put("items", List.of("b"));
                      return Map.of("result", "b");
                    }));
    assertThat(
            run(List.of(a, b), mode, Map.of("items", new ArrayList<>(List.of("old"))))
                .session
                .state()
                .get("items"))
        .isEqualTo(List.of("b"));
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void directScalarDeltas_keepCallOrder(ToolExecutionMode mode) {
    BaseTool a =
        tool(
            "a",
            context ->
                Single.fromCallable(
                    () -> {
                      context.eventActions().stateDelta().put("done", false);
                      return Map.of("result", "a");
                    }));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.eventActions().stateDelta().put("done", true);
                      return Map.of("result", "b");
                    }));
    assertThat(run(List.of(a, b), mode, Map.of("done", false)).session.state().get("done"))
        .isEqualTo(true);
  }

  @ParameterizedTest
  @EnumSource(
      value = ToolExecutionMode.class,
      names = {"PARALLEL", "PARALLEL_SUBSCRIBE"})
  void liveFunctionResponseMerge_preservesLatestSharedStateWrite(ToolExecutionMode mode) {
    CompletableSubject bWritten = CompletableSubject.create();
    BaseTool a =
        tool(
            "a",
            context ->
                bWritten.andThen(
                    Single.fromCallable(
                        () -> {
                          List<Object> updated =
                              new ArrayList<>((List<?>) context.state().get("items"));
                          updated.add("a");
                          context.state().put("items", updated);
                          return Map.of("result", "a");
                        })));
    BaseTool b =
        tool(
            "b",
            context ->
                Single.fromCallable(
                    () -> {
                      context.state().put("items", List.of("b"));
                      bWritten.onComplete();
                      return Map.of("result", "b");
                    }));
    Fixture fixture = fixture(List.of(a, b), mode, Map.of());
    InvocationContext context =
        InvocationContext.builder()
            .invocationId("live_state")
            .agent(fixture.agent)
            .session(fixture.session)
            .sessionService(fixture.service)
            .runConfig(fixture.config)
            .build();
    var subscriber =
        Functions.handleFunctionCallsLive(
                context,
                Event.builder()
                    .invocationId(context.invocationId())
                    .author("root")
                    .content(fixture.calls)
                    .build(),
                Map.of("a", a, "b", b))
            .test()
            .awaitDone(5, TimeUnit.SECONDS);
    subscriber.assertComplete().assertNoErrors().assertValueCount(1);
    Event response = subscriber.values().get(0);
    fixture.service.appendEvent(fixture.session, response).blockingGet();
    assertThat(fixture.reload().state().get("items")).isEqualTo(List.of("b", "a"));
    assertThat(response.functionResponses().stream().map(fr -> fr.id().orElse("")).toList())
        .containsExactly("a_call", "b_call")
        .inOrder();
  }

  private static Result run(
      List<BaseTool> tools, ToolExecutionMode mode, Map<String, Object> initialState) {
    Fixture fixture = fixture(tools, mode, initialState);
    var subscriber =
        fixture
            .runner
            .runAsync(
                "user",
                fixture.session.id(),
                Content.fromParts(Part.fromText("go")),
                fixture.config)
            .test()
            .awaitDone(5, TimeUnit.SECONDS);
    subscriber.assertComplete().assertNoErrors();
    Event response =
        subscriber.values().stream()
            .filter(event -> !event.functionResponses().isEmpty())
            .findFirst()
            .orElseThrow();
    return new Result(fixture.reload(), response);
  }

  private static Fixture fixture(
      List<BaseTool> tools, ToolExecutionMode mode, Map<String, Object> initialState) {
    List<Part> calls =
        tools.stream()
            .map(
                tool ->
                    Part.builder()
                        .functionCall(
                            FunctionCall.builder()
                                .id(tool.name() + "_call")
                                .name(tool.name())
                                .args(Map.of())
                                .build())
                        .build())
            .toList();
    Content callContent = Content.builder().role("model").parts(calls).build();
    TestLlm model =
        TestLlm.create(
            LlmResponse.builder().content(callContent).build(),
            LlmResponse.builder()
                .content(Content.builder().role("model").parts(Part.fromText("done")).build())
                .build());
    LlmAgent agent = LlmAgent.builder().name("root").model(model).tools(tools).build();
    InMemorySessionService service = new InMemorySessionService();
    Runner runner =
        Runner.builder()
            .app(App.builder().name(APP).rootAgent(agent).build())
            .sessionService(service)
            .build();
    Session session = service.createSession(APP, "user", initialState, null).blockingGet();
    return new Fixture(
        runner,
        agent,
        service,
        session,
        RunConfig.builder().setToolExecutionMode(mode).build(),
        callContent);
  }

  private record Result(Session session, Event response) {}

  private record Fixture(
      Runner runner,
      LlmAgent agent,
      InMemorySessionService service,
      Session session,
      RunConfig config,
      Content calls) {
    Session reload() {
      return service.getSession(APP, "user", session.id(), Optional.empty()).blockingGet();
    }
  }

  private static BaseTool tool(
      String name, Function<ToolContext, Single<Map<String, Object>>> action) {
    return new BaseTool(name, "Offline fixture") {
      @Override
      public Optional<FunctionDeclaration> declaration() {
        return Optional.of(
            FunctionDeclaration.builder().name(name()).description("Offline fixture").build());
      }

      @Override
      public Single<Map<String, Object>> runAsync(Map<String, Object> args, ToolContext context) {
        return action.apply(context);
      }
    };
  }
}
