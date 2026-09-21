// @effect-diagnostics nodeBuiltinImport:off
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { PiAgentSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const settings = Schema.decodeSync(PiAgentSettings)({
  enabled: true,
  binaryPath: process.execPath,
});
const instanceId = ProviderInstanceId.make("piAgent");
const modelSelection = { instanceId, model: "mock/model" };
const mockPath = NodeURL.fileURLToPath(
  new URL("../../scripts/pi-session-mock-agent.mjs", import.meta.url),
);
const setup = Effect.fn("PiTextGeneration.test.setup")(function* (text: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-text-test-" });
  const log = path.join(cwd, "args.json");
  const service = yield* makePiTextGeneration(
    settings,
    instanceId,
    { ...process.env, T3_PI_MOCK_TEXT: text, T3_PI_MOCK_SPAWN_LOG: log },
    [mockPath],
  );
  return { service, cwd, log, fs };
});
const decodeArgs = Schema.decodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));

describe("Pi text generation", () => {
  it.effect("generates titles with tools and persistence disabled", () =>
    Effect.gen(function* () {
      const test = yield* setup('{"title":"Fix Pi integration"}');
      const result = yield* test.service.generateThreadTitle({
        cwd: test.cwd,
        message: "Fix the integration",
        modelSelection,
      });
      assert.equal(result.title, "Fix Pi integration");
      const args = yield* decodeArgs(yield* test.fs.readFileString(test.log));
      for (const flag of [
        "--no-tools",
        "--no-session",
        "--no-extensions",
        "--no-skills",
        "--no-prompt-templates",
      ])
        assert.include(args, flag);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("validates commit message structure", () =>
    Effect.gen(function* () {
      const test = yield* setup('{"subject":"fix: integrate Pi","body":"Handle RPC events."}');
      const result = yield* test.service.generateCommitMessage({
        cwd: test.cwd,
        branch: "feature/pi",
        stagedSummary: "Pi adapter",
        stagedPatch: "diff",
        modelSelection,
      });
      assert.equal(result.subject, "fix: integrate Pi");
      assert.equal(result.body, "Handle RPC events.");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects invalid structured output", () =>
    Effect.gen(function* () {
      const test = yield* setup("not JSON");
      const error = yield* test.service
        .generateThreadTitle({ cwd: test.cwd, message: "Fix Pi", modelSelection })
        .pipe(Effect.flip);
      assert.equal(error._tag, "TextGenerationError");
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
