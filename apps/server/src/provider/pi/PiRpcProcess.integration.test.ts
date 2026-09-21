// @effect-diagnostics nodeBuiltinImport:off
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { runPiRpcBatch } from "./PiRpcProcess.ts";

it.effect("exchanges JSONL with a real subprocess across UTF-8 and record boundaries", () =>
  Effect.gen(function* () {
    const replies = yield* runPiRpcBatch({
      binaryPath: process.execPath,
      launchArgs: [
        NodeURL.fileURLToPath(new URL("../../../scripts/pi-rpc-mock-agent.mjs", import.meta.url)),
      ],
      commands: [{ type: "get_state" }, { type: "get_available_models" }],
    });
    assert.deepStrictEqual(
      replies.map((reply) => reply.command),
      ["get_available_models", "get_state"],
    );
    for (const reply of replies) {
      assert.isTrue(reply.success);
      if (reply.success) assert.deepStrictEqual(reply.data, { label: "Modèle π" });
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);
