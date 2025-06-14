# Restate Java/Kotlin SDK — Compact Guide for OpenAI Codex Agents

## Purpose

Equip code‑generation agents with a high‑level map of Restate’s Java/Kotlin SDK so they can scaffold handlers,
workflows, and tests without searching the docs. **No sample code is included; only names, semantics, and relationships.
**

---

## Quick Facts

* **Runtime requirement**: JDK 17+, reachable Restate server (local Docker or cluster)
* **Transport**: HTTP/gRPC (handled by SDK’s `RestateHttpServer` and client)
* **Default port**:9080

---

## Core Abstractions

| Concept            | Annotation / Marker                       | Main Context Type                           | Key Properties                                                                                |
|--------------------|-------------------------------------------|---------------------------------------------|-----------------------------------------------------------------------------------------------|
| **Service**        | `@Service` + `@Handler`                   | `Context`                                   | Stateless, at‑least‑once durable execution, independent concurrent handlers                   |
| **Virtual Object** | `@VirtualObject` + `@Handler` / `@Shared` | `ObjectContext` / `SharedObjectContext`     | Key‑scoped K/V state, single‑threaded mutation, optional concurrent read‑only shared handlers |
| **Workflow**       | `@Workflow` (class & main method)         | `WorkflowContext` / `SharedWorkflowContext` | Durable orchestration, await/call/sleep, single entry handler (`run`) per execution           |

---

## Context Capabilities (non‑exhaustive)

* **Remote calls**: `ctx.call(handlerRef, arg)` → returns `Completion<T>`
* **Journaled side‑effects**: `ctx.run(type, supplier)` stores result for replay
* **Durable timers**: `ctx.sleep(duration)`
* **Promises & coordination**: `ctx.promise()`, `ctx.await(...)`, `ctx.any(...)`
* **Key access**: `ctx.key()` for current object/workflow identifier
* **State store (Virtual Objects & Workflows)**: `ctx.state().get(key)`, `ctx.state().set(key, value)`

---

## Execution Model

* SDK records deterministic steps and side‑effect results in an **execution log**.
* On retry, log is replayed; external calls are **not re‑issued**, results are re‑used.
* Guarantees **exactly‑once** effects and at‑least‑once code execution.

---

## Failure & Retry Semantics

* Automatic exponential back‑off until success.
* Customizable policies (per‑call overrides or annotation metadata).
* Log replay provides idempotency, so explicit deduplication seldom required.

---

## State Management

* Virtual Objects & Workflows expose a transactional key‑value store via `ctx.state()`.
* Writes are committed atomically at successful handler completion.

---

## Scheduling & Timers

* `ctx.sleep` schedules wake‑ups from milliseconds to months with crash‑safe guarantees.

---

## Promises & Awaitables

* `Completion<T>` acts like a distributed `Future` that survives restarts.
* Share completions across services; resolve once, await many times.

---

## Typical Patterns

1. **Orchestration**: Workflow calls multiple downstream handlers, awaits completions, applies compensations on error.
2. **Event‑driven Entity**: Virtual Object processes commands, mutates state, emits events.
3. **Async Task Queue**: Service schedules long‑running work via handler call and sleeps or re‑attaches.

---

## Client Access

* Java client can synchronously await, fire‑and‑forget, or signal/query workflows.
* Supports retries, time‑outs, and header propagation.

---

## Serving & Deployment

* Bind implementations to an `Endpoint`, then start `RestateHttpServer.listen(...)`.
* Multiple services/objects/workflows may share one process.

---

## Testing

* **LocalRestate** harness spins up an ephemeral server for JUnit tests.
* Validate determinism by forcing restarts and comparing outputs.

---

## Serialization

* JSON codec by default; pluggable registry for custom formats.
* Inputs, outputs, and errors must be serializable.

---

## Glossary

* **Handler**: Method exposed via Restate.
* **Execution Log**: Ordered record of actions & results enabling replay.
* **Completion**: Durable promise representing the eventual result of an async call.

---

## Reference Links (no code)

* Overview: docs.restate.dev/develop/java/overview
* Concepts: docs.restate.dev/concepts/
* GitHub repository: github.com/restatedev/sdk-java

---

_End of AGENTS.md_

