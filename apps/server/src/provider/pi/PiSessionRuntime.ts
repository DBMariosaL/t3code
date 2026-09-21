import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { PiRpcProcessError } from "./PiRpcProcess.ts";
import {
  decodePiRpcLine,
  encodePiRpcCommand,
  isPiRpcResponse,
  splitPiJsonLines,
  type PiRpcEvent,
} from "./PiRpcProtocol.ts";

export interface PiSessionRuntimeOptions {
  readonly binaryPath: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
}
export type PiRuntimeEvent =
  | { readonly _tag: "Event"; readonly event: PiRpcEvent }
  | { readonly _tag: "Barrier"; readonly done: Deferred.Deferred<void, PiRpcProcessError> };

/** A single scoped Pi subprocess, with independent response and event consumers. */
export const makePiSessionRuntime = Effect.fn("PiSessionRuntime.make")(function* (
  options: PiSessionRuntimeOptions,
) {
  const scope = yield* Scope.Scope;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const resolved = yield* resolveSpawnCommand(
    options.binaryPath,
    [...options.args, "--mode", "rpc"],
    options.environment ? { env: options.environment, extendEnv: true } : {},
  );
  const child = yield* spawner.spawn(
    ChildProcess.make(resolved.command, resolved.args, {
      cwd: options.cwd,
      ...(options.environment ? { env: options.environment, extendEnv: true } : {}),
      shell: resolved.shell,
    }),
  );
  const outgoing = yield* Queue.unbounded<string>();
  const incoming = yield* Queue.unbounded<PiRuntimeEvent, PiRpcProcessError>();
  const pending = new Map<
    string,
    { command: string; result: Deferred.Deferred<unknown, PiRpcProcessError> }
  >();
  let nextId = 0;
  let closed: PiRpcProcessError | undefined;
  const barriers = new Set<Deferred.Deferred<void, PiRpcProcessError>>();

  const fail = Effect.fn("PiSessionRuntime.fail")(function* (error: PiRpcProcessError) {
    if (closed) return;
    closed = error;
    yield* Effect.forEach(pending.values(), (entry) => Deferred.fail(entry.result, error), {
      discard: true,
    });
    pending.clear();
    yield* Effect.forEach(barriers, (done) => Deferred.fail(done, error), { discard: true });
    yield* Queue.fail(incoming, error);
    yield* Queue.shutdown(outgoing);
    yield* child.kill().pipe(Effect.ignore);
  });
  yield* Effect.addFinalizer(() =>
    fail(new PiRpcProcessError({ operation: "close", detail: "Pi session closed." })),
  );

  const notify = Effect.fn("PiSessionRuntime.notify")(function* (
    command: string,
    fields: Readonly<Record<string, unknown>> = {},
  ) {
    if (closed) return yield* closed;
    yield* Queue.offer(outgoing, encodePiRpcCommand(command, fields));
  });
  const request = Effect.fn("PiSessionRuntime.request")(function* (
    command: string,
    fields: Readonly<Record<string, unknown>> = {},
    timeoutMs = 30_000,
  ) {
    if (closed) return yield* closed;
    const id = `t3-${++nextId}`;
    const result = yield* Deferred.make<unknown, PiRpcProcessError>();
    pending.set(id, { command, result });
    return yield* notify(command, { ...fields, id }).pipe(
      Effect.andThen(Deferred.await(result)),
      Effect.timeout(timeoutMs),
      Effect.catchTag("TimeoutError", () =>
        Effect.gen(function* () {
          const error = new PiRpcProcessError({
            operation: command,
            detail: "Pi did not acknowledge the request before the deadline.",
          });
          yield* fail(error);
          return yield* error;
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          pending.delete(id);
        }),
      ),
    );
  });
  let lineState = { remainder: "" };
  const read = child.stdout.pipe(
    Stream.decodeText(),
    Stream.flatMap((chunk) => {
      const split = splitPiJsonLines(lineState, chunk);
      lineState = split.state;
      return Stream.fromIterable(split.lines);
    }),
    Stream.mapEffect(decodePiRpcLine),
    Stream.runForEach((message) =>
      Effect.gen(function* () {
        if (!isPiRpcResponse(message)) {
          yield* Queue.offer(incoming, { _tag: "Event", event: message });
          return;
        }
        const entry = message.id ? pending.get(message.id) : undefined;
        if (!entry) return;
        if (entry.command !== message.command) {
          yield* fail(
            new PiRpcProcessError({
              operation: entry.command,
              detail: "Pi response command does not match its request.",
            }),
          );
        } else if (message.success) {
          yield* Deferred.succeed(entry.result, message.data);
        } else {
          yield* Deferred.fail(
            entry.result,
            new PiRpcProcessError({
              operation: entry.command,
              detail: message.error ?? "Pi rejected the request.",
            }),
          );
        }
      }),
    ),
    Effect.andThen(
      fail(new PiRpcProcessError({ operation: "read", detail: "Pi closed its output stream." })),
    ),
  );
  const write = Stream.run(Stream.encodeText(Stream.fromQueue(outgoing)), child.stdin);
  for (const worker of [read, write, Stream.runDrain(child.stderr)]) {
    yield* worker.pipe(
      Effect.catch((cause) =>
        fail(
          new PiRpcProcessError({ operation: "transport", detail: "Pi transport failed.", cause }),
        ),
      ),
      Effect.forkIn(scope),
    );
  }
  const drainEvents = Effect.gen(function* () {
    if (closed) return yield* closed;
    const done = yield* Deferred.make<void, PiRpcProcessError>();
    barriers.add(done);
    yield* Queue.offer(incoming, { _tag: "Barrier", done });
    yield* Deferred.await(done).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          barriers.delete(done);
        }),
      ),
    );
  });
  return { request, notify, drainEvents, events: Stream.fromQueue(incoming) };
});

export type PiSessionRuntime = Effect.Success<ReturnType<typeof makePiSessionRuntime>>;
