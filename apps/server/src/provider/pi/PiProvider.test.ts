import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { PiAgentSettings } from "@t3tools/contracts";

import {
  buildPiModels,
  buildPiSlashCommands,
  checkPiProviderStatus,
  decodePiDiscovery,
} from "./PiProvider.ts";
import { ChildProcessSpawner } from "effect/unstable/process";
const defaultPiSettings = Schema.decodeSync(PiAgentSettings)({});

describe("PiProvider", () => {
  it("advertises only probed reasoning levels and native Responses speed controls", () => {
    const models = buildPiModels(
      [
        { provider: "openai", id: "supported", api: "openai-responses" },
        { provider: "other", id: "model", api: "anthropic-messages" },
        { provider: "openai", id: "unprobed", api: "openai-responses" },
      ],
      null,
      [],
      new Map([
        ["openai/supported", ["low", "high"]],
        ["other/model", ["off"]],
      ]),
    );
    const descriptors = models[0]?.capabilities?.optionDescriptors;
    assert.equal(descriptors?.[0]?.id, "reasoningEffort");
    if (descriptors?.[0]?.type === "select") {
      assert.deepStrictEqual(
        descriptors[0].options.map((option) => option.id),
        ["low", "high"],
      );
      assert.isUndefined(descriptors[0].currentValue);
    }
    assert.equal(descriptors?.[1]?.id, "serviceTier");
    assert.equal(models[1]?.capabilities?.optionDescriptors?.length, 1);
    assert.deepStrictEqual(models[2]?.capabilities?.optionDescriptors, []);
  });
  it.effect("does not spawn a process for a disabled provider", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkPiProviderStatus(defaultPiSettings).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
        ),
      );
      assert.equal(snapshot.status, "disabled");
      assert.equal(snapshot.auth.status, "unknown");
    }),
  );

  it.effect("returns typed failures for missing and rejected discovery commands", () =>
    Effect.gen(function* () {
      const missing = yield* Effect.flip(decodePiDiscovery([]));
      assert.equal(missing._tag, "PiRpcProtocolError");
      const rejected = yield* Effect.flip(
        decodePiDiscovery([
          { type: "response", command: "get_state", success: false, error: "unavailable" },
        ]),
      );
      assert.equal(rejected._tag, "PiRpcProtocolError");
    }),
  );

  it("deduplicates models while retaining providers with the same model id", () => {
    const models = buildPiModels(
      [
        { provider: "first", id: "shared" },
        { provider: "first", id: "shared" },
        { provider: "second", id: "shared" },
      ],
      null,
      ["custom/model"],
    );
    assert.deepStrictEqual(
      models.map((model) => model.slug),
      ["first/shared", "second/shared", "custom/model"],
    );
    assert.isTrue(models[2]?.isCustom);
    assert.isFalse(models.some((model) => model.isDefault === true));
  });
  it("qualifies model ids by their Pi provider and marks the current model", () => {
    const models = buildPiModels(
      [
        { provider: "anthropic", id: "claude-sonnet", name: "Claude Sonnet", reasoning: true },
        { provider: "openai", id: "gpt-5.4", reasoning: false },
      ],
      { provider: "openai", id: "gpt-5.4" },
      [],
    );

    assert.equal(models[0]?.slug, "anthropic/claude-sonnet");
    assert.equal(models[0]?.subProvider, "anthropic");
    assert.deepStrictEqual(models[0]?.capabilities?.optionDescriptors, []);
    assert.isTrue(models[1]?.isDefault);
  });

  it("preserves discovered slash commands including extension commands", () => {
    assert.deepStrictEqual(
      buildPiSlashCommands([
        { name: "reload", description: " Reload resources ", source: "extension" },
      ]),
      [{ name: "reload", description: "Reload resources" }],
    );
  });
});
