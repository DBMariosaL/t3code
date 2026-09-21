import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import { makePiSessionRuntime } from "./PiSessionRuntime.ts";

const setup = Effect.fn("PiSessionRuntime.test.setup")(function* () {
  const output = yield* Queue.unbounded<Uint8Array>();
  const written = yield* Queue.unbounded<string>();
  let killed = false;
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(42),
        exitCode: Effect.never,
        isRunning: Effect.sync(() => !killed),
        kill: () =>
          Effect.sync(() => {
            killed = true;
          }),
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach((bytes: Uint8Array) =>
          Queue.offer(written, new TextDecoder().decode(bytes)),
        ),
        stdout: Stream.fromQueue(output),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
  const runtime = yield* makePiSessionRuntime({
    binaryPath: process.execPath,
    args: [],
    cwd: process.cwd(),
  }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
  const reply = (id: string, command: string, data: unknown) =>
    Queue.offer(
      output,
      new TextEncoder().encode(
        JSON.stringify({ type: "response", success: true, id, command, data }) + "\n",
      ),
    );
  return { runtime, written, reply, killed: () => killed };
});

describe("Pi persistent RPC transport", () => {
  it.effect("correlates concurrent responses without requiring an event consumer", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.runtime.request("get_state").pipe(Effect.forkChild);
      assert.include(yield* Queue.take(test.written), '"id":"t3-1"');
      const second = yield* test.runtime.request("get_commands").pipe(Effect.forkChild);
      assert.include(yield* Queue.take(test.written), '"id":"t3-2"');
      yield* test.reply("t3-2", "get_commands", { commands: [] });
      assert.deepStrictEqual(yield* Fiber.join(second), { commands: [] });
      yield* test.reply("t3-1", "get_state", { sessionId: "session" });
      assert.deepStrictEqual(yield* Fiber.join(first), { sessionId: "session" });
      assert.isFalse(test.killed());
    }),
  );

  it.effect("a request timeout terminates the process and fails all pending work", () =>
    Effect.gen(function* () {
      const test = yield* setup();
      const first = yield* test.runtime
        .request("prompt", {}, 1000)
        .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
      yield* Queue.take(test.written);
      const second = yield* test.runtime
        .request("get_state")
        .pipe(Effect.asVoid, Effect.flip, Effect.forkChild);
      yield* Queue.take(test.written);
      const barrierQueued = yield* Deferred.make<void>();
      yield* Stream.runForEach(test.runtime.events, (event) =>
        event._tag === "Barrier" ? Deferred.succeed(barrierQueued, undefined) : Effect.void,
      ).pipe(Effect.forkChild);
      const barrier = yield* test.runtime.drainEvents.pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(barrierQueued);
      yield* TestClock.adjust("1 second");
      assert.equal((yield* Fiber.join(first)).operation, "prompt");
      assert.equal((yield* Fiber.join(second)).operation, "prompt");
      assert.equal((yield* Fiber.join(barrier)).operation, "prompt");
      assert.isTrue(test.killed());
      const error = yield* test.runtime.request("get_state").pipe(Effect.asVoid, Effect.flip);
      assert.equal(error._tag, "PiRpcProcessError");
    }),
  );
});
