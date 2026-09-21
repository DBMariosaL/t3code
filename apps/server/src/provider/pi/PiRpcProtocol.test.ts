import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  decodePiCommandsData,
  decodePiModelsData,
  decodePiRpcLine,
  encodePiRpcCommand,
  splitPiJsonLines,
} from "./PiRpcProtocol.ts";

describe("PiRpcProtocol", () => {
  it("keeps incomplete JSONL records between chunks", () => {
    const first = splitPiJsonLines({ remainder: "" }, '{"type":"agent_start"}\n{"type":');
    assert.deepStrictEqual(first.lines, ['{"type":"agent_start"}']);
    assert.equal(first.state.remainder, '{"type":');

    const second = splitPiJsonLines(first.state, '"agent_end"}\r\n');
    assert.deepStrictEqual(second.lines, ['{"type":"agent_end"}']);
    assert.equal(second.state.remainder, "");
  });

  it.effect("decodes responses and rejects non-protocol JSON", () =>
    Effect.gen(function* () {
      const response = yield* decodePiRpcLine(
        '{"type":"response","command":"get_state","success":true,"data":{"model":null}}',
      );
      assert.equal(response.type, "response");

      const invalid = yield* Effect.flip(decodePiRpcLine('{"hello":"world"}'));
      assert.equal(invalid._tag, "PiRpcProtocolError");
    }),
  );

  it.effect("validates model and command discovery payloads", () =>
    Effect.gen(function* () {
      const models = yield* decodePiModelsData({
        models: [
          {
            provider: "anthropic",
            id: "claude-sonnet-4-5",
            name: "Claude Sonnet 4.5",
            reasoning: true,
            input: ["text", "image"],
            contextWindow: 200_000,
            maxTokens: 64_000,
          },
        ],
      });
      const commands = yield* decodePiCommandsData({
        commands: [{ name: "reload", description: "Reload extensions", source: "extension" }],
      });

      assert.equal(models.models[0]?.provider, "anthropic");
      assert.equal(commands.commands[0]?.name, "reload");
    }),
  );

  it.effect("rejects malformed response envelopes instead of accepting them as events", () =>
    Effect.gen(function* () {
      for (const line of [
        '{"type":"response"}',
        '{"type":"response","command":"get_state","success":"yes"}',
        '{"type":"response","command":"get_state","success":false,"error":42}',
        "{",
        "",
      ]) {
        const error = yield* Effect.flip(decodePiRpcLine(line));
        assert.equal(error._tag, "PiRpcProtocolError");
      }
    }),
  );

  it.effect("preserves event payloads and accepts acknowledgements without data", () =>
    Effect.gen(function* () {
      const event = yield* decodePiRpcLine('{"type":"message_update","delta":"bonjour π"}');
      assert.deepStrictEqual(event, { type: "message_update", delta: "bonjour π" });
      const ack = yield* decodePiRpcLine('{"type":"response","command":"abort","success":true}');
      assert.equal(ack.type, "response");
    }),
  );

  it("escapes embedded newlines and prevents fields from replacing the command", () => {
    const encoded = encodePiRpcCommand("prompt", { type: "abort", message: "first\nsecond" });
    assert.equal(encoded.split("\n").length, 2);
    assert.deepStrictEqual(JSON.parse(encoded), { type: "prompt", message: "first\nsecond" });
  });

  it("encodes one LF-terminated command", () => {
    assert.equal(
      encodePiRpcCommand("set_model", { provider: "openai", modelId: "gpt-5.4" }),
      '{"provider":"openai","modelId":"gpt-5.4","type":"set_model"}\n',
    );
  });
});
