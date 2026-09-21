import * as Schema from "effect/Schema";

const Message = Schema.Struct({
  role: Schema.String,
  content: Schema.optional(Schema.Unknown),
  stopReason: Schema.optional(Schema.String),
  errorMessage: Schema.optional(Schema.String),
});
export const PiMessageEvent = Schema.Struct({
  type: Schema.Literals(["message_start", "message_end"]),
  message: Message,
});
export const PiMessageUpdate = Schema.Struct({
  type: Schema.Literal("message_update"),
  assistantMessageEvent: Schema.Struct({
    type: Schema.String,
    contentIndex: Schema.Int,
    delta: Schema.optional(Schema.String),
  }),
});
export const PiToolEvent = Schema.Struct({
  type: Schema.Literals(["tool_execution_start", "tool_execution_update", "tool_execution_end"]),
  toolCallId: Schema.String,
  toolName: Schema.String,
  args: Schema.optional(Schema.Unknown),
  partialResult: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Unknown),
  isError: Schema.optional(Schema.Boolean),
});
export const PiUiRequest = Schema.Struct({
  type: Schema.Literal("extension_ui_request"),
  id: Schema.String,
  method: Schema.String,
  title: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  options: Schema.optional(Schema.Array(Schema.String)),
});
export type PiUiRequest = typeof PiUiRequest.Type;
export const isPiMessageEvent = Schema.is(PiMessageEvent);
export const isPiMessageUpdate = Schema.is(PiMessageUpdate);
export const isPiToolEvent = Schema.is(PiToolEvent);
export const decodePiUiRequest = Schema.decodeUnknownEffect(PiUiRequest);
export const PiResumeCursor = Schema.Struct({
  version: Schema.Literal(1),
  sessionFile: Schema.String.check(Schema.isMinLength(1)),
});
export const decodePiResume = Schema.decodeUnknownEffect(PiResumeCursor);
export const PiMessages = Schema.Struct({ messages: Schema.Array(Message) });
export const decodePiMessages = Schema.decodeUnknownEffect(PiMessages);
