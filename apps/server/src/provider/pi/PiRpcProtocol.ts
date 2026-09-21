import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "@t3tools/contracts";

export const PiRpcModel = Schema.Struct({
  provider: TrimmedNonEmptyString,
  id: TrimmedNonEmptyString,
  name: Schema.optional(TrimmedNonEmptyString),
  reasoning: Schema.optional(Schema.Boolean),
  api: Schema.optional(Schema.String),
  input: Schema.optional(Schema.Array(Schema.String)),
  contextWindow: Schema.optional(Schema.Number),
  maxTokens: Schema.optional(Schema.Number),
});
export type PiRpcModel = typeof PiRpcModel.Type;

export const PiRpcCommand = Schema.Struct({
  name: TrimmedNonEmptyString,
  description: Schema.optional(Schema.String),
  source: Schema.optional(Schema.String),
});
export type PiRpcCommand = typeof PiRpcCommand.Type;

export const PiRpcState = Schema.Struct({
  model: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        provider: TrimmedNonEmptyString,
        id: TrimmedNonEmptyString,
        api: Schema.optional(Schema.String),
      }),
    ),
  ),
  thinkingLevel: Schema.optional(Schema.String),
  isStreaming: Schema.optional(Schema.Boolean),
  isCompacting: Schema.optional(Schema.Boolean),
  pendingMessageCount: Schema.optional(Schema.Number),
  sessionFile: Schema.optional(Schema.NullOr(Schema.String)),
  sessionId: Schema.optional(Schema.String),
});
export type PiRpcState = typeof PiRpcState.Type;

const PiRpcSuccessResponse = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.String),
  command: TrimmedNonEmptyString,
  success: Schema.Literal(true),
  data: Schema.optional(Schema.Unknown),
});

const PiRpcFailureResponse = Schema.Struct({
  type: Schema.Literal("response"),
  id: Schema.optional(Schema.String),
  command: TrimmedNonEmptyString,
  success: Schema.Literal(false),
  error: Schema.optional(Schema.String),
});

export const PiRpcResponse = Schema.Union([PiRpcSuccessResponse, PiRpcFailureResponse]);
export type PiRpcResponse = typeof PiRpcResponse.Type;

export const PiRpcEvent = Schema.StructWithRest(
  Schema.Struct({
    type: TrimmedNonEmptyString.check(Schema.makeFilter((type) => type !== "response")),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);
export type PiRpcEvent = typeof PiRpcEvent.Type;

export const PiRpcMessage = Schema.Union([PiRpcResponse, PiRpcEvent]);
export type PiRpcMessage = typeof PiRpcMessage.Type;
export const isPiRpcResponse = Schema.is(PiRpcResponse);

export class PiRpcProtocolError extends Schema.TaggedError<PiRpcProtocolError>()(
  "PiRpcProtocolError",
  {
    detail: Schema.String,
    line: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Pi RPC protocol error: ${this.detail}`;
  }
}

const decodeMessageJson = Schema.decodeUnknownEffect(Schema.fromJsonString(PiRpcMessage));

export const decodePiRpcLine = Effect.fn("PiRpcProtocol.decodeLine")(function* (line: string) {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return yield* new PiRpcProtocolError({ detail: "Received an empty JSONL record.", line });
  }
  return yield* decodeMessageJson(trimmed).pipe(
    Effect.mapError(
      (cause) => new PiRpcProtocolError({ detail: "Received invalid JSONL from Pi.", line, cause }),
    ),
  );
});

export interface PiJsonLinesState {
  readonly remainder: string;
}

export interface PiJsonLinesChunk {
  readonly lines: ReadonlyArray<string>;
  readonly state: PiJsonLinesState;
}

/**
 * Splits Pi's strict LF-delimited protocol without treating a partial UTF-8
 * stream chunk as a complete message. A preceding CR is tolerated for test
 * wrappers and Windows shims, although Pi itself writes LF-only records.
 */
export function splitPiJsonLines(state: PiJsonLinesState, chunk: string): PiJsonLinesChunk {
  const segments = `${state.remainder}${chunk}`.split("\n");
  const remainder = segments.pop() ?? "";
  return {
    lines: segments.map((line) => (line.endsWith("\r") ? line.slice(0, -1) : line)),
    state: { remainder },
  };
}

export function encodePiRpcCommand(
  command: string,
  fields: Readonly<Record<string, unknown>> = {},
): string {
  return `${JSON.stringify({ ...fields, type: command })}\n`;
}

const ModelsData = Schema.Struct({ models: Schema.Array(PiRpcModel) });
const CommandsData = Schema.Struct({ commands: Schema.Array(PiRpcCommand) });

export const decodePiModelsData = Schema.decodeUnknownEffect(ModelsData);
export const decodePiCommandsData = Schema.decodeUnknownEffect(CommandsData);
export const decodePiStateData = Schema.decodeUnknownEffect(PiRpcState);
export const decodePiThinkingLevelsData = Schema.decodeUnknownEffect(
  Schema.Struct({ levels: Schema.Array(TrimmedNonEmptyString) }),
);
