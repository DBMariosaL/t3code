import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";

import { runPiRpcBatch } from "./PiRpcProcess.ts";

const reply = (id: number, command: string) =>
  JSON.stringify({ type: "response", id: `t3-discovery-${id}`, command, success: true, data: {} }) +
  "\n";

const fixture = Effect.fn("PiRpcProcess.test.fixture")(function* (
  output: Stream.Stream<Uint8Array>,
) {
  const written = yield* Deferred.make<void>();
  let input = "";
  let released = false;
  const spawner = ChildProcessSpawner.make(() =>
    Effect.acquireRelease(
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(42),
          exitCode: Effect.never,
          isRunning: Effect.sync(() => !released),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.forEach((bytes: Uint8Array) =>
            Effect.sync(() => {
              input += new TextDecoder().decode(bytes);
            }).pipe(Effect.andThen(Deferred.succeed(written, undefined))),
          ),
          stdout: Stream.fromEffect(Deferred.await(written)).pipe(Stream.flatMap(() => output)),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
      () =>
        Effect.sync(() => {
          released = true;
        }),
    ),
  );
  return { spawner, written, input: () => input, released: () => released };
});

const commands = [{ type: "get_state" }, { type: "get_available_models" }];

describe("Pi RPC discovery process", () => {
  it.effect("correlates out-of-order replies and releases a process that remains alive", () =>
    Effect.gen(function* () {
      const output = reply(1, "get_available_models") + reply(0, "get_state");
      const mock = yield* fixture(
        Stream.concat(Stream.make(new TextEncoder().encode(output)), Stream.never),
      );
      const replies = yield* runPiRpcBatch({ binaryPath: process.execPath, commands }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mock.spawner),
      );
      assert.equal(replies.length, 2);
      assert.equal(replies[0]?.command, "get_available_models");
      assert.include(mock.input(), '"id":"t3-discovery-0"');
      assert.isTrue(mock.released());
    }),
  );

  it.effect("does not count duplicate responses or uncorrelated events as replies", () =>
    Effect.gen(function* () {
      const output = '{"type":"agent_start"}\n' + reply(0, "get_state") + reply(0, "get_state");
      const mock = yield* fixture(Stream.make(new TextEncoder().encode(output)));
      const error = yield* runPiRpcBatch({ binaryPath: process.execPath, commands }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mock.spawner),
        Effect.flip,
      );
      assert.include(error.detail, "before replying");
      assert.isTrue(mock.released());
    }),
  );

  it.effect("rejects a response with a mismatched command", () =>
    Effect.gen(function* () {
      const mock = yield* fixture(Stream.make(new TextEncoder().encode(reply(0, "abort"))));
      const error = yield* runPiRpcBatch({ binaryPath: process.execPath, commands }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mock.spawner),
        Effect.flip,
      );
      assert.include(error.detail, "does not match");
      assert.isTrue(mock.released());
    }),
  );

  it.effect("times out and releases an unresponsive process", () =>
    Effect.gen(function* () {
      const mock = yield* fixture(Stream.never);
      const running = yield* runPiRpcBatch({
        binaryPath: process.execPath,
        commands,
        timeoutMs: 1000,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mock.spawner),
        Effect.flip,
        Effect.forkChild,
      );
      yield* Deferred.await(mock.written);
      yield* TestClock.adjust("1 second");
      const error = yield* Fiber.join(running);
      assert.include(error.detail, "timed out");
      assert.isTrue(mock.released());
    }),
  );

  it.effect("releases its process when the caller is interrupted", () =>
    Effect.gen(function* () {
      const mock = yield* fixture(Stream.never);
      const running = yield* runPiRpcBatch({ binaryPath: process.execPath, commands }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mock.spawner),
        Effect.forkChild,
      );
      yield* Deferred.await(mock.written);
      yield* Fiber.interrupt(running);
      assert.isTrue(mock.released());
    }),
  );
});
