import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  decodePiRpcLine,
  encodePiRpcCommand,
  isPiRpcResponse,
  splitPiJsonLines,
  type PiRpcResponse,
} from "./PiRpcProtocol.ts";

export class PiRpcProcessError extends Schema.TaggedError<PiRpcProcessError>()(
  "PiRpcProcessError",
  {
    operation: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi RPC process failed during ${this.operation}: ${this.detail}`;
  }
}

export interface PiRpcBatchCommand {
  readonly type: string;
  readonly fields?: Readonly<Record<string, unknown>>;
}
const isPiRpcProcessError = Schema.is(PiRpcProcessError);

export interface PiRpcBatchOptions {
  readonly binaryPath: string;
  readonly cwd?: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly launchArgs?: ReadonlyArray<string>;
  readonly commands: ReadonlyArray<PiRpcBatchCommand>;
  readonly timeoutMs?: number;
}

/** Owns a disposable discovery process until every correlated reply arrives. */
export const runPiRpcBatch = Effect.fn("PiRpcProcess.runBatch")(
  function* (options: PiRpcBatchOptions) {
    if (options.commands.length === 0) return [];
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const resolved = yield* resolveSpawnCommand(
      options.binaryPath,
      [...(options.launchArgs ?? []), "--mode", "rpc", "--no-session", "--no-extensions"],
      options.environment ? { env: options.environment, extendEnv: true } : {},
    );
    const child = yield* spawner.spawn(
      ChildProcess.make(resolved.command, resolved.args, {
        ...(options.cwd ? { cwd: options.cwd } : {}),
        ...(options.environment ? { env: options.environment, extendEnv: true } : {}),
        shell: resolved.shell,
      }),
    );
    const expected = new Map(
      options.commands.map((command, index) => [`t3-discovery-${index}`, command.type]),
    );
    const input = options.commands
      .map((command, index) =>
        encodePiRpcCommand(command.type, { ...command.fields, id: `t3-discovery-${index}` }),
      )
      .join("");

    // Keep stdin open. EOF can shut Pi down before asynchronous replies arrive.
    const write = Stream.run(
      Stream.encodeText(Stream.concat(Stream.make(input), Stream.never)),
      child.stdin,
    ).pipe(Effect.andThen(Effect.never));
    const responses: PiRpcResponse[] = [];
    let state = { remainder: "" };
    const read = child.stdout.pipe(
      Stream.decodeText(),
      Stream.flatMap((chunk) => {
        const split = splitPiJsonLines(state, chunk);
        state = split.state;
        return Stream.fromIterable(split.lines);
      }),
      Stream.mapEffect(decodePiRpcLine),
      Stream.filter(isPiRpcResponse),
      Stream.tap((response) =>
        Effect.gen(function* () {
          if (response.id === undefined || !expected.has(response.id)) return;
          if (expected.get(response.id) !== response.command) {
            return yield* new PiRpcProcessError({
              operation: "batch",
              detail: "Pi replied with a command that does not match its request id.",
            });
          }
          expected.delete(response.id);
          responses.push(response);
        }),
      ),
      Stream.takeUntil(() => expected.size === 0),
      Stream.runDrain,
      Effect.andThen(
        Effect.suspend(() =>
          expected.size === 0
            ? Effect.succeed(responses)
            : Effect.fail(
                new PiRpcProcessError({
                  operation: "batch",
                  detail: "Pi closed stdout before replying to every discovery request.",
                }),
              ),
        ),
      ),
    );
    // Drain stderr without accumulating diagnostics or logging secrets.
    const drain = Stream.runDrain(child.stderr).pipe(Effect.andThen(Effect.never));
    return yield* Effect.raceFirst(read, Effect.raceFirst(write, drain)).pipe(
      Effect.timeoutOption(options.timeoutMs ?? 15_000),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new PiRpcProcessError({ operation: "batch", detail: "Pi discovery timed out." }),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );
  },
  Effect.scoped,
  Effect.mapError((cause) =>
    isPiRpcProcessError(cause)
      ? cause
      : new PiRpcProcessError({ operation: "batch", detail: "Pi RPC discovery failed.", cause }),
  ),
);
