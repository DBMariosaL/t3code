// @effect-diagnostics nodeBuiltinImport:off
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiAgentSettings,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { makePiAdapter } from "./PiAdapter.ts";

const settings = Schema.decodeSync(PiAgentSettings)({
  enabled: true,
  binaryPath: process.execPath,
});
const mockPath = NodeURL.fileURLToPath(
  new URL("../../../scripts/pi-session-mock-agent.mjs", import.meta.url),
);
const decodeEvent = Schema.decodeUnknownEffect(ProviderRuntimeEvent);
const instanceId = ProviderInstanceId.make("piAgent");

const setup = Effect.fn("PiAdapter.test.setup")(function* (environment: NodeJS.ProcessEnv = {}) {
  const fs = yield* FileSystem.FileSystem;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-adapter-test-" });
  const adapter = yield* makePiAdapter(settings, {
    instanceId,
    commandPrefix: [mockPath],
    environment: { ...process.env, ...environment },
  });
  const completed = yield* Deferred.make<ProviderRuntimeEvent>();
  const asked = yield* Deferred.make<void>();
  const exited = yield* Deferred.make<void>();
  const events: ProviderRuntimeEvent[] = [];
  yield* Stream.runForEach(adapter.streamEvents, (event) =>
    Effect.gen(function* () {
      yield* decodeEvent(event);
      events.push(event);
      if (event.type === "turn.completed") yield* Deferred.succeed(completed, event);
      if (event.type === "user-input.requested") yield* Deferred.succeed(asked, undefined);
      if (event.type === "session.exited") yield* Deferred.succeed(exited, undefined);
    }),
  ).pipe(Effect.forkChild);
  const threadId = ThreadId.make("pi-thread");
  const session = yield* adapter.startSession({ threadId, cwd, runtimeMode: "full-access" });
  return { adapter, session, threadId, cwd, events, completed, asked, exited };
});

describe("Pi session adapter", () => {
  it.effect("applies reasoning after model changes, reload and resume", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pi-options-test-" });
      const log = `${directory}/commands.jsonl`;
      const test = yield* setup({ T3_PI_MOCK_LOG: log });
      const modelSelection = {
        instanceId,
        model: "mock/other",
        options: [{ id: "reasoningEffort", value: "high" }],
      };
      const turn = yield* test.adapter.sendTurn({
        threadId: test.threadId,
        input: "hello",
        modelSelection,
      });
      yield* Deferred.await(test.completed);
      yield* test.adapter.sendTurn({ threadId: test.threadId, input: "/reload" });
      yield* test.adapter.stopSession(test.threadId);
      yield* test.adapter.startSession({
        threadId: test.threadId,
        cwd: test.cwd,
        runtimeMode: "full-access",
        resumeCursor: turn.resumeCursor,
        modelSelection,
      });
      const commands = (yield* fs.readFileString(log))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      assert.deepStrictEqual(
        commands
          .filter((command) => command.type === "set_thinking_level")
          .map((command) => command.level),
        ["high", "high", "high"],
      );
      assert.equal(commands.filter((command) => command.type === "set_model").length, 2);
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects unavailable and silently clamped reasoning before prompting", () =>
    Effect.gen(function* () {
      for (const [environment, value] of [
        [{}, "max"],
        [{ T3_PI_MOCK_CLAMP: "1" }, "high"],
      ] as const) {
        const test = yield* setup(environment);
        const error = yield* test.adapter
          .sendTurn({
            threadId: test.threadId,
            input: "hello",
            modelSelection: {
              instanceId,
              model: "mock/model",
              options: [{ id: "reasoningEffort", value }],
            },
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterValidationError");
        assert.equal((yield* test.adapter.readThread(test.threadId)).turns.length, 0);
        yield* test.adapter.stopAll();
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects busy reload and plan mode without disturbing the active turn", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "hold" });
      for (const input of [
        { input: "/reload" },
        { input: "plan", interactionMode: "plan" as const },
        {
          input: "change settings",
          modelSelection: {
            instanceId,
            model: "mock/model",
            options: [{ id: "reasoningEffort", value: "high" }],
          },
        },
      ]) {
        const error = yield* test.adapter
          .sendTurn({ threadId: test.threadId, ...input })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ProviderAdapterValidationError");
        assert.equal((yield* test.adapter.listSessions())[0]?.activeTurnId, turn.turnId);
      }
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects priority on unsupported APIs before sending a prompt", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const error = yield* test.adapter
        .sendTurn({
          threadId: test.threadId,
          input: "hello",
          modelSelection: {
            instanceId,
            model: "mock/model",
            options: [{ id: "serviceTier", value: "priority" }],
          },
        })
        .pipe(Effect.flip);
      assert.equal(error._tag, "ProviderAdapterValidationError");
      assert.equal((yield* test.adapter.readThread(test.threadId)).turns.length, 0);
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects protected sessions and missing resume files", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const threadId = ThreadId.make("invalid-session");
      const protectedError = yield* test.adapter
        .startSession({ threadId, cwd: test.cwd, runtimeMode: "approval-required" })
        .pipe(Effect.flip);
      assert.equal(protectedError._tag, "ProviderAdapterValidationError");
      const resumeError = yield* test.adapter
        .startSession({
          threadId,
          cwd: test.cwd,
          runtimeMode: "full-access",
          resumeCursor: { version: 1, sessionFile: `${test.cwd}/missing.jsonl` },
        })
        .pipe(Effect.flip);
      assert.equal(resumeError._tag, "ProviderAdapterValidationError");
      assert.isFalse(yield* test.adapter.hasSession(threadId));
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("streams messages and tools and settles exactly once", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "hello" });
      const completed = yield* Deferred.await(test.completed);
      assert.equal(completed.turnId, turn.turnId);
      assert.equal(test.events.filter((event) => event.type === "turn.completed").length, 1);
      assert.isTrue(
        test.events.some(
          (event) => event.type === "content.delta" && event.payload.delta === "hello",
        ),
      );
      assert.isTrue(
        test.events.some(
          (event) =>
            event.type === "item.completed" && event.payload.itemType === "command_execution",
        ),
      );
      assert.equal((yield* test.adapter.listSessions())[0]?.status, "ready");
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps a retrying turn open and reuses its id for steering", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "hold" });
      assert.equal((yield* test.adapter.listSessions())[0]?.status, "running");
      assert.isFalse(test.events.some((event) => event.type === "turn.completed"));
      const steer = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "continue" });
      yield* Deferred.await(test.completed);
      assert.equal(steer.turnId, turn.turnId);
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("interrupts the active turn and ignores a stale turn id", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "hold" });
      yield* test.adapter.interruptTurn(test.threadId, TurnId.make("stale-turn"));
      assert.equal((yield* test.adapter.listSessions())[0]?.status, "running");
      yield* test.adapter.interruptTurn(test.threadId, turn.turnId);
      const completed = yield* Deferred.await(test.completed);
      assert.equal(completed.type, "turn.completed");
      if (completed.type === "turn.completed") assert.equal(completed.payload.state, "interrupted");
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reloads through the extension without adding a model prompt", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const before = test.session.resumeCursor;
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "/reload" });
      yield* Deferred.await(test.completed);
      assert.deepStrictEqual(turn.resumeCursor, before);
      assert.equal((yield* test.adapter.readThread(test.threadId)).turns.length, 0);
      assert.isTrue(
        test.events.some(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "Pi resources reloaded.",
        ),
      );
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not report a failed reload as success", () =>
    Effect.gen(function* () {
      const test = yield* setup({ T3_PI_MOCK_FAIL_RELOAD: "1" });
      yield* test.adapter.sendTurn({ threadId: test.threadId, input: "/reload" }).pipe(Effect.flip);
      const completed = yield* Deferred.await(test.completed);
      if (completed.type === "turn.completed") assert.equal(completed.payload.state, "failed");
      assert.isFalse(
        test.events.some(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "Pi resources reloaded.",
        ),
      );
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("resumes saved conversation and rolls back through Pi fork", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const turn = yield* test.adapter.sendTurn({ threadId: test.threadId, input: "hello" });
      yield* Deferred.await(test.completed);
      yield* test.adapter.stopSession(test.threadId);
      yield* test.adapter.startSession({
        threadId: test.threadId,
        cwd: test.cwd,
        runtimeMode: "full-access",
        resumeCursor: turn.resumeCursor,
      });
      assert.equal((yield* test.adapter.readThread(test.threadId)).turns.length, 1);
      assert.equal((yield* test.adapter.rollbackThread(test.threadId, 1)).turns.length, 0);
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("answers extension dialogs without blocking RPC responses", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      yield* test.adapter.sendTurn({ threadId: test.threadId, input: "ask" });
      yield* Deferred.await(test.asked);
      yield* test.adapter.respondToUserInput(test.threadId, ApprovalRequestId.make("question-1"), {
        "question-1": "B",
      });
      yield* Deferred.await(test.completed);
      assert.isTrue(
        test.events.some((event) => event.type === "content.delta" && event.payload.delta === "B"),
      );
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports a crashed Pi process and permits restarting the thread", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      yield* test.adapter.sendTurn({ threadId: test.threadId, input: "crash" }).pipe(Effect.flip);
      yield* Deferred.await(test.exited);
      assert.isFalse(yield* test.adapter.hasSession(test.threadId));
      yield* test.adapter.startSession({
        threadId: test.threadId,
        cwd: test.cwd,
        runtimeMode: "full-access",
      });
      assert.isTrue(yield* test.adapter.hasSession(test.threadId));
      yield* test.adapter.stopAll();
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
