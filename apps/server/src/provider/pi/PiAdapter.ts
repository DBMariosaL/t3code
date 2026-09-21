import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  RuntimeRequestId,
  TurnId,
  type PiAgentSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
  type CanonicalItemType,
  type ModelSelection,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { PI_BRIDGE_SOURCE } from "./PiBridge.ts";
import { PI_FAST_APIS } from "./PiProvider.ts";
import { PiRpcProcessError } from "./PiRpcProcess.ts";
import {
  decodePiStateData,
  decodePiCommandsData,
  decodePiThinkingLevelsData,
  type PiRpcEvent,
} from "./PiRpcProtocol.ts";
import { makePiSessionRuntime, type PiSessionRuntime } from "./PiSessionRuntime.ts";
import {
  decodePiMessages,
  decodePiResume,
  decodePiUiRequest,
  isPiMessageEvent,
  isPiMessageUpdate,
  isPiToolEvent,
  type PiUiRequest,
} from "./PiSessionEvents.ts";

const PROVIDER = ProviderDriverKind.make("piAgent");
type Adapter = ProviderAdapterShape<ProviderAdapterError>;
type EventBody<E = ProviderRuntimeEvent> = E extends ProviderRuntimeEvent
  ? Omit<E, "eventId" | "createdAt" | "provider" | "providerInstanceId" | "threadId">
  : never;
interface Session {
  session: ProviderSession;
  runtime: PiSessionRuntime;
  scope: Scope.Closeable;
  lock: Semaphore.Semaphore;
  stopped: boolean;
  turn: TurnId | undefined;
  error: string | undefined;
  interrupted: boolean;
  messageIndex: number;
  items: Map<string, { type: CanonicalItemType; text: string; input?: unknown }>;
  requests: Map<string, PiUiRequest>;
  modelSelection: ModelSelection | undefined;
  optionsFile: string;
}

const isAdapterError = Schema.is(
  Schema.Union([
    ProviderAdapterRequestError,
    ProviderAdapterSessionNotFoundError,
    ProviderAdapterValidationError,
  ]),
);
const isRpcProcessError = Schema.is(PiRpcProcessError);
const mapError =
  (method: string) =>
  (cause: unknown): ProviderAdapterError =>
    isAdapterError(cause)
      ? cause
      : new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: isRpcProcessError(cause) ? cause.message : `Pi ${method} failed.`,
          cause,
        });
const mutationResult = Schema.Struct({ cancelled: Schema.Boolean });
const decodeMutation = Schema.decodeUnknownEffect(mutationResult);
const forkMessages = Schema.Struct({
  messages: Schema.Array(Schema.Struct({ entryId: Schema.String, text: Schema.String })),
});
const decodeForkMessages = Schema.decodeUnknownEffect(forkMessages);
const encodeOptions = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      serviceTier: Schema.optional(Schema.String),
    }),
  ),
);

export const makePiAdapter = Effect.fn("PiAdapter.make")(function* (
  settings: PiAgentSettings,
  options: {
    readonly instanceId?: ProviderInstanceId;
    readonly environment?: NodeJS.ProcessEnv;
    readonly commandPrefix?: ReadonlyArray<string>;
  } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const crypto = yield* Crypto.Crypto;
  const identity = yield* crypto.randomUUIDv4;
  const instanceId = options.instanceId ?? ProviderInstanceId.make("piAgent");
  const sessions = new Map<ThreadId, Session>();
  const lifecycle = yield* Semaphore.make(1);
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  let sequence = 0;
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const emit = Effect.fn("PiAdapter.emit")(function* (ctx: Session, body: EventBody) {
    yield* PubSub.publish(events, {
      ...body,
      eventId: EventId.make(`${identity}-${++sequence}`),
      createdAt: yield* now,
      provider: PROVIDER,
      providerInstanceId: instanceId,
      threadId: ctx.session.threadId,
    });
  });
  const requireSession = Effect.fn("PiAdapter.requireSession")(function* (threadId: ThreadId) {
    const ctx = sessions.get(threadId);
    if (!ctx || ctx.stopped)
      return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
    return ctx;
  });
  const refresh = Effect.fn("PiAdapter.refresh")(function* (ctx: Session) {
    const state = yield* ctx.runtime.request("get_state").pipe(Effect.flatMap(decodePiStateData));
    ctx.session = {
      ...ctx.session,
      ...(state.sessionFile
        ? { resumeCursor: { version: 1, sessionFile: state.sessionFile } }
        : {}),
      ...(state.model ? { model: `${state.model.provider}/${state.model.id}` } : {}),
      updatedAt: yield* now,
    };
    return state;
  });
  const finish = Effect.fn("PiAdapter.finish")(function* (ctx: Session) {
    const turnId = ctx.turn;
    if (!turnId) return;
    for (const [id, item] of ctx.items) {
      yield* emit(ctx, {
        type: "item.completed",
        turnId,
        itemId: RuntimeItemId.make(id),
        payload: {
          itemType: item.type,
          status: ctx.error || ctx.interrupted ? "failed" : "completed",
        },
      });
    }
    ctx.items.clear();
    ctx.turn = undefined;
    const { activeTurnId: _, ...session } = ctx.session;
    ctx.session = { ...session, status: "ready", updatedAt: yield* now };
    yield* emit(ctx, {
      type: "turn.completed",
      turnId,
      payload: {
        state: ctx.interrupted ? "interrupted" : ctx.error ? "failed" : "completed",
        ...(ctx.error ? { errorMessage: ctx.error } : {}),
      },
    });
    yield* emit(ctx, { type: "session.state.changed", payload: { state: "ready" } });
  });

  const consume = Effect.fn("PiAdapter.consume")(function* (ctx: Session, event: PiRpcEvent) {
    if (ctx.stopped) return;
    if (event.type === "extension_error") {
      ctx.error = typeof event.error === "string" ? event.error : "Pi extension failed.";
      return;
    }
    const turnId = ctx.turn;
    if (event.type === "extension_ui_request") {
      const request = yield* decodePiUiRequest(event);
      if (!["confirm", "select", "input", "editor"].includes(request.method)) return;
      ctx.requests.set(request.id, request);
      yield* emit(ctx, {
        type: "user-input.requested",
        ...(turnId ? { turnId } : {}),
        requestId: RuntimeRequestId.make(request.id),
        payload: {
          questions: [
            {
              id: request.id,
              header: "Pi",
              question: request.title || request.message || "Pi extension input",
              options: (request.method === "confirm" ? ["Yes", "No"] : (request.options ?? [])).map(
                (label) => ({ label, description: label }),
              ),
            },
          ],
        },
      });
      return;
    }
    if (!turnId) return;
    if (event.type === "agent_settled") {
      const state = yield* refresh(ctx);
      if (
        ctx.stopped ||
        ctx.turn !== turnId ||
        state.isStreaming ||
        state.isCompacting ||
        (state.pendingMessageCount ?? 0) > 0
      )
        return;
      yield* finish(ctx);
      return;
    }
    if (isPiMessageEvent(event) && event.message.role === "assistant") {
      if (event.type === "message_start") ctx.messageIndex += 1;
      if (event.type === "message_end") {
        ctx.error =
          event.message.stopReason === "error"
            ? event.message.errorMessage || "Pi model request failed."
            : undefined;
        if (event.message.stopReason === "aborted") ctx.interrupted = true;
        for (const [id, item] of ctx.items) {
          if (item.type !== "assistant_message" && item.type !== "reasoning") continue;
          yield* emit(ctx, {
            type: "item.completed",
            turnId,
            itemId: RuntimeItemId.make(id),
            payload: { itemType: item.type, status: "completed" },
          });
          ctx.items.delete(id);
        }
      }
    } else if (isPiMessageUpdate(event)) {
      const update = event.assistantMessageEvent;
      if (update.type !== "text_delta" && update.type !== "thinking_delta") return;
      const itemType = update.type === "text_delta" ? "assistant_message" : "reasoning";
      const id = `${turnId}-message-${ctx.messageIndex}-${update.contentIndex}`;
      if (!ctx.items.has(id)) {
        ctx.items.set(id, { type: itemType, text: "" });
        yield* emit(ctx, {
          type: "item.started",
          turnId,
          itemId: RuntimeItemId.make(id),
          payload: { itemType },
        });
      }
      yield* emit(ctx, {
        type: "content.delta",
        turnId,
        itemId: RuntimeItemId.make(id),
        payload: {
          streamKind: itemType === "reasoning" ? "reasoning_text" : "assistant_text",
          delta: update.delta ?? "",
          contentIndex: update.contentIndex,
        },
      });
    } else if (isPiToolEvent(event)) {
      const id = `${turnId}-tool-${event.toolCallId}`;
      const itemType =
        event.toolName === "bash"
          ? "command_execution"
          : ["edit", "write"].includes(event.toolName)
            ? "file_change"
            : "dynamic_tool_call";
      if (event.type === "tool_execution_start")
        ctx.items.set(id, { type: itemType, text: "", input: event.args });
      yield* emit(ctx, {
        type:
          event.type === "tool_execution_start"
            ? "item.started"
            : event.type === "tool_execution_end"
              ? "item.completed"
              : "item.updated",
        turnId,
        itemId: RuntimeItemId.make(id),
        payload: {
          itemType,
          title: event.toolName,
          status:
            event.type === "tool_execution_end"
              ? event.isError
                ? "failed"
                : "completed"
              : "inProgress",
          data: {
            toolCallId: event.toolCallId,
            toolName: event.toolName,
            input: ctx.items.get(id)?.input ?? event.args,
            result: event.result ?? event.partialResult,
          },
        },
      });
      if (event.type === "tool_execution_end") ctx.items.delete(id);
    }
  });

  const stop = Effect.fn("PiAdapter.stop")(function* (ctx: Session) {
    ctx.stopped = true;
    if (ctx.turn) {
      ctx.interrupted = true;
      yield* finish(ctx);
    }
    yield* Scope.close(ctx.scope, Exit.void);
    sessions.delete(ctx.session.threadId);
    yield* emit(ctx, { type: "session.exited", payload: { exitKind: "graceful" } });
  });
  yield* Effect.addFinalizer(() => Effect.forEach(sessions.values(), stop, { discard: true }));

  const setModel = Effect.fn("PiAdapter.setModel")(function* (
    ctx: Session,
    selection?: ModelSelection,
  ) {
    if (!selection || selection.instanceId !== instanceId) return;
    const separator = selection.model.indexOf("/");
    if (separator <= 0 || separator === selection.model.length - 1) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "setModel",
        issue: "Pi models must use provider/model identifiers.",
      });
    }
    if (selection.model !== ctx.session.model) {
      yield* ctx.runtime.request("set_model", {
        provider: selection.model.slice(0, separator),
        modelId: selection.model.slice(separator + 1),
      });
      const state = yield* refresh(ctx);
      if (`${state.model?.provider}/${state.model?.id}` !== selection.model) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "setModel",
          issue: "Pi did not apply the requested model.",
        });
      }
    }
    const effort = getModelSelectionStringOptionValue(selection, "reasoningEffort");
    if (effort !== undefined) {
      const { levels } = yield* ctx.runtime
        .request("get_available_thinking_levels")
        .pipe(Effect.flatMap(decodePiThinkingLevelsData));
      if (!levels.includes(effort)) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "reasoningEffort",
          issue: `Pi does not support reasoning level ${effort} for ${selection.model}.`,
        });
      }
      yield* ctx.runtime.request("set_thinking_level", { level: effort });
      const state = yield* refresh(ctx);
      if (state.thinkingLevel !== effort) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "reasoningEffort",
          issue: `Pi applied ${state.thinkingLevel ?? "an unknown level"} instead of ${effort}.`,
        });
      }
    }
    const serviceTier = getModelSelectionStringOptionValue(selection, "serviceTier");
    if (serviceTier !== undefined) {
      const state = yield* refresh(ctx);
      if (
        !["default", "priority"].includes(serviceTier) ||
        (serviceTier === "priority" && !PI_FAST_APIS.includes(state.model?.api ?? ""))
      ) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "serviceTier",
          issue: "Pi Fast mode requires a native OpenAI Responses or Codex Responses model.",
        });
      }
    }
    yield* fs.writeFileString(ctx.optionsFile, yield* encodeOptions({ serviceTier }));
    ctx.modelSelection = selection;
  });
  const startSession: Adapter["startSession"] = (input) =>
    lifecycle.withPermit(
      Effect.gen(function* () {
        if (!input.cwd || (input.provider !== undefined && input.provider !== PROVIDER)) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Pi requires a working directory and the piAgent provider.",
          });
        }
        if (
          input.runtimeMode !== "full-access" ||
          (input.sandboxMode && input.sandboxMode !== "danger-full-access")
        ) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue:
              "Pi currently requires Full access. Approval and sandbox modes are not implemented.",
          });
        }
        const previous = sessions.get(input.threadId);
        if (previous) yield* stop(previous);
        const scope = yield* Scope.make();
        let transferred = false;
        yield* Effect.addFinalizer(() =>
          transferred ? Effect.void : Scope.close(scope, Exit.void),
        );
        const directory = yield* fs
          .makeTempDirectoryScoped({ prefix: "t3-pi-" })
          .pipe(Effect.provideService(Scope.Scope, scope));
        const extension = path.join(directory, "bridge.mjs");
        const optionsFile = path.join(directory, "options.json");
        yield* fs.writeFileString(extension, PI_BRIDGE_SOURCE);
        yield* fs.writeFileString(optionsFile, "{}");
        const resume =
          input.resumeCursor === undefined ? undefined : yield* decodePiResume(input.resumeCursor);
        if (resume && !(yield* fs.exists(resume.sessionFile)))
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "resume",
            issue: "The saved Pi session file no longer exists.",
          });
        const runtime = yield* makePiSessionRuntime({
          binaryPath: settings.binaryPath,
          cwd: input.cwd,
          args: [
            ...(options.commandPrefix ?? []),
            "--extension",
            extension,
            ...(resume ? ["--session", resume.sessionFile] : []),
          ],
          ...(options.environment ? { environment: options.environment } : {}),
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(Scope.Scope, scope),
        );
        const createdAt = yield* now;
        const ctx: Session = {
          session: {
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            cwd: input.cwd,
            status: "ready",
            createdAt,
            updatedAt: createdAt,
          },
          runtime,
          scope,
          lock: yield* Semaphore.make(1),
          stopped: false,
          turn: undefined,
          error: undefined,
          interrupted: false,
          messageIndex: 0,
          items: new Map(),
          requests: new Map(),
          modelSelection: undefined,
          optionsFile,
        };
        yield* refresh(ctx);
        const commands = yield* runtime
          .request("get_commands")
          .pipe(Effect.flatMap(decodePiCommandsData));
        if (!commands.commands.some((command) => command.name === "t3-reload"))
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "Pi did not load the T3 reload extension.",
          });
        yield* setModel(ctx, input.modelSelection);
        yield* Stream.runForEach(runtime.events, (entry) =>
          entry._tag === "Barrier"
            ? Deferred.succeed(entry.done, undefined)
            : consume(ctx, entry.event),
        ).pipe(
          Effect.catch((cause) =>
            Effect.gen(function* () {
              if (ctx.stopped) return;
              ctx.error = String(cause);
              yield* finish(ctx);
              ctx.stopped = true;
              ctx.session = { ...ctx.session, status: "error", lastError: String(cause) };
              yield* emit(ctx, {
                type: "session.exited",
                payload: { exitKind: "error", recoverable: true, reason: String(cause) },
              });
            }),
          ),
          Effect.forkIn(scope),
        );
        sessions.set(input.threadId, ctx);
        transferred = true;
        yield* emit(ctx, {
          type: "session.started",
          payload: { resume: ctx.session.resumeCursor },
        });
        yield* emit(ctx, { type: "session.state.changed", payload: { state: "ready" } });
        yield* emit(ctx, { type: "thread.started", payload: {} });
        return ctx.session;
      }).pipe(Effect.scoped, Effect.mapError(mapError("startSession"))),
    );

  const sendTurn: Adapter["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const ctx = yield* requireSession(input.threadId);
      return yield* ctx.lock.withPermit(
        Effect.gen(function* () {
          if (input.attachments?.length || input.interactionMode === "plan") {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Pi attachments and plan mode are not supported yet.",
            });
          }
          const reload = input.input?.trim() === "/reload";
          if (reload && ctx.turn)
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "reload",
              issue: "Wait for Pi to finish or interrupt it before reloading resources.",
            });
          if (
            ctx.turn &&
            input.modelSelection &&
            (input.modelSelection.model !== ctx.session.model ||
              ["reasoningEffort", "serviceTier"].some(
                (id) =>
                  getModelSelectionStringOptionValue(input.modelSelection, id) !==
                  getModelSelectionStringOptionValue(ctx.modelSelection, id),
              ))
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "setModel",
              issue: "Wait for the current turn before changing models or model options.",
            });
          }
          if (!ctx.turn) yield* setModel(ctx, input.modelSelection);
          const steering = ctx.turn !== undefined;
          const turnId = ctx.turn ?? TurnId.make(`${identity}-turn-${++sequence}`);
          if (!steering) {
            ctx.turn = turnId;
            ctx.error = undefined;
            ctx.interrupted = false;
            ctx.session = {
              ...ctx.session,
              status: "running",
              activeTurnId: turnId,
              updatedAt: yield* now,
            };
            yield* emit(ctx, { type: "turn.started", turnId, payload: {} });
            yield* emit(ctx, { type: "session.state.changed", payload: { state: "running" } });
          }
          yield* ctx.runtime
            .request("prompt", {
              message: reload ? "/t3-reload" : (input.input ?? ""),
              ...(steering ? { streamingBehavior: "steer" } : {}),
            })
            .pipe(
              Effect.catch((cause) =>
                Effect.gen(function* () {
                  if (!ctx.stopped && !steering && ctx.turn === turnId) {
                    ctx.error = cause.message;
                    yield* finish(ctx);
                  }
                  return yield* cause;
                }),
              ),
            );
          if (reload) {
            yield* refresh(ctx);
            yield* ctx.runtime.drainEvents;
            if (ctx.error) {
              yield* finish(ctx);
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "reload",
                issue: ctx.error,
              });
            }
            yield* setModel(ctx, ctx.modelSelection).pipe(
              Effect.catch((cause) =>
                Effect.gen(function* () {
                  ctx.error = String(cause);
                  yield* finish(ctx);
                  return yield* cause;
                }),
              ),
            );
            const itemId = RuntimeItemId.make(`${turnId}-reload`);
            yield* emit(ctx, {
              type: "item.started",
              turnId,
              itemId,
              payload: { itemType: "assistant_message" },
            });
            yield* emit(ctx, {
              type: "content.delta",
              turnId,
              itemId,
              payload: { streamKind: "assistant_text", delta: "Pi resources reloaded." },
            });
            yield* emit(ctx, {
              type: "item.completed",
              turnId,
              itemId,
              payload: { itemType: "assistant_message", status: "completed" },
            });
            yield* finish(ctx);
          } else if (!steering) {
            const state = yield* refresh(ctx);
            yield* ctx.runtime.drainEvents;
            if (
              !ctx.stopped &&
              ctx.turn === turnId &&
              state.isStreaming === false &&
              state.isCompacting !== true &&
              (state.pendingMessageCount ?? 0) === 0
            )
              yield* finish(ctx);
          }
          return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
        }),
      );
    }).pipe(Effect.mapError(mapError("sendTurn")));

  const readThread = Effect.fn("PiAdapter.readThread")(function* (threadId: ThreadId) {
    const ctx = yield* requireSession(threadId);
    const result = yield* ctx.runtime
      .request("get_messages")
      .pipe(Effect.flatMap(decodePiMessages));
    const turns: Array<{ id: TurnId; items: unknown[] }> = [];
    for (const message of result.messages) {
      if (message.role === "user")
        turns.push({ id: TurnId.make(`${threadId}-pi-${turns.length}`), items: [] });
      turns.at(-1)?.items.push(message);
    }
    return { threadId, turns };
  });
  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    listSessions: () =>
      Effect.sync(() =>
        [...sessions.values()].filter((ctx) => !ctx.stopped).map((ctx) => ctx.session),
      ),
    hasSession: (threadId) =>
      Effect.sync(() => sessions.has(threadId) && !sessions.get(threadId)?.stopped),
    stopSession: (threadId) =>
      lifecycle.withPermit(
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (ctx) yield* stop(ctx);
        }),
      ),
    stopAll: () => lifecycle.withPermit(Effect.forEach(sessions.values(), stop, { discard: true })),
    interruptTurn: (threadId, turnId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        if (!ctx.turn || (turnId && ctx.turn !== turnId)) return;
        ctx.interrupted = true;
        yield* ctx.runtime.request("abort");
      }).pipe(Effect.mapError(mapError("abort"))),
    readThread: (threadId) => readThread(threadId).pipe(Effect.mapError(mapError("readThread"))),
    rollbackThread: (threadId, count) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return yield* ctx.lock.withPermit(
          Effect.gen(function* () {
            if (ctx.turn || !Number.isInteger(count) || count < 1)
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "rollback",
                issue: "Rollback requires an idle session and a positive turn count.",
              });
            const forks = yield* ctx.runtime
              .request("get_fork_messages")
              .pipe(Effect.flatMap(decodeForkMessages));
            const target = forks.messages.at(-count);
            if (!target)
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "rollback",
                issue: "Not enough Pi turns to roll back.",
              });
            const result = yield* ctx.runtime
              .request("fork", { entryId: target.entryId })
              .pipe(Effect.flatMap(decodeMutation));
            if (result.cancelled)
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "rollback",
                issue: "A Pi extension cancelled rollback.",
              });
            yield* refresh(ctx);
            return yield* readThread(threadId);
          }),
        );
      }).pipe(Effect.mapError(mapError("rollback"))),
    respondToRequest: () =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "approval",
          issue: "Pi approval policies are not implemented.",
        }),
      ),
    respondToUserInput: (threadId, requestId, answers) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const request = ctx.requests.get(requestId);
        if (!request)
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "input",
            issue: "Unknown Pi input request.",
          });
        const raw = answers[requestId];
        const value = typeof raw === "string" ? raw : Array.isArray(raw) ? raw[0] : undefined;
        yield* ctx.runtime.notify("extension_ui_response", {
          id: requestId,
          ...(request.method === "confirm"
            ? { confirmed: value === "Yes" }
            : typeof value === "string"
              ? { value }
              : { cancelled: true }),
        });
        ctx.requests.delete(requestId);
        yield* emit(ctx, {
          type: "user-input.resolved",
          requestId: RuntimeRequestId.make(requestId),
          payload: { answers },
        });
      }).pipe(Effect.mapError(mapError("input"))),
    streamEvents: Stream.fromPubSub(events),
  } satisfies Adapter;
});
