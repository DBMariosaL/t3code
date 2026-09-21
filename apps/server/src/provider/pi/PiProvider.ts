import {
  type PiAgentSettings,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
  type ProviderOptionDescriptor,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { compareSemverVersions } from "@t3tools/shared/semver";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { ChildProcess } from "effect/unstable/process";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
} from "../providerSnapshot.ts";
import {
  decodePiCommandsData,
  decodePiModelsData,
  decodePiStateData,
  decodePiThinkingLevelsData,
  PiRpcProtocolError,
  type PiRpcCommand,
  type PiRpcModel,
  type PiRpcResponse,
} from "./PiRpcProtocol.ts";
import { runPiRpcBatch } from "./PiRpcProcess.ts";
import { makePiSessionRuntime } from "./PiSessionRuntime.ts";

const PI_PRESENTATION = {
  displayName: "Pi Agent",
  badgeLabel: "Full access only",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
export const PI_FAST_APIS = ["openai-responses", "openai-codex-responses"];

const responseData = Effect.fn("PiProvider.responseData")(function* (
  responses: ReadonlyArray<PiRpcResponse>,
  command: string,
) {
  const response = responses.find((entry) => entry.command === command);
  if (!response || !response.success) {
    return yield* new PiRpcProtocolError({
      detail: response?.error ?? `Pi did not return a successful ${command} response.`,
    });
  }
  return response.data;
});

export const decodePiDiscovery = Effect.fn("PiProvider.decodeDiscovery")(function* (
  responses: ReadonlyArray<PiRpcResponse>,
) {
  const state = yield* decodePiStateData(yield* responseData(responses, "get_state"));
  const models = yield* decodePiModelsData(yield* responseData(responses, "get_available_models"));
  const commands = yield* decodePiCommandsData(yield* responseData(responses, "get_commands"));
  return { state, models: models.models, commands: commands.commands };
});

export function buildPiModels(
  discovered: ReadonlyArray<PiRpcModel>,
  currentModel: { readonly provider: string; readonly id: string } | null,
  customModels: PiAgentSettings["customModels"],
  thinkingLevels: ReadonlyMap<string, ReadonlyArray<string>> = new Map(),
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const builtIn: ServerProviderModel[] = [];
  for (const model of discovered) {
    const slug = `${model.provider}/${model.id}`;
    if (seen.has(slug)) continue;
    seen.add(slug);
    const optionDescriptors: ProviderOptionDescriptor[] = [];
    const levels = thinkingLevels.get(slug);
    if (levels?.length) {
      optionDescriptors.push({
        id: "reasoningEffort",
        label: "Reasoning",
        type: "select",
        options: levels.map((level) => ({ id: level, label: level })),
      });
      if (model.api && PI_FAST_APIS.includes(model.api)) {
        optionDescriptors.push({
          id: "serviceTier",
          label: "Speed",
          type: "select",
          description:
            "Fast requests priority service and may increase usage costs. Speed is not guaranteed.",
          options: [
            { id: "default", label: "Default", isDefault: true },
            {
              id: "priority",
              label: "Fast",
              description:
                "Requests priority service; may increase usage costs. Speed is not guaranteed.",
            },
          ],
        });
      }
    }
    builtIn.push({
      slug,
      name: model.name ?? model.id,
      subProvider: model.provider,
      isCustom: false,
      ...(currentModel?.provider === model.provider && currentModel.id === model.id
        ? { isDefault: true }
        : {}),
      capabilities: createModelCapabilities({ optionDescriptors }),
    });
  }
  return providerModelsFromSettings(builtIn, customModels, EMPTY_CAPABILITIES);
}

export function buildPiSlashCommands(
  commands: ReadonlyArray<PiRpcCommand>,
): ReadonlyArray<ServerProviderSlashCommand> {
  return commands.map((command) => ({
    name: command.name,
    ...(command.description?.trim() ? { description: command.description.trim() } : {}),
  }));
}

export const buildInitialPiProviderSnapshot = Effect.fn("buildInitialPiProviderSnapshot")(
  function* (settings: PiAgentSettings) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt: DateTime.formatIso(yield* DateTime.now),
      models: providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES),
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled
          ? "Checking Pi Agent availability..."
          : "Pi Agent is disabled in T3 Code settings.",
      },
    });
  },
);

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  settings: PiAgentSettings,
  environment: NodeJS.ProcessEnv = process.env,
  cwd?: string,
) {
  if (!settings.enabled) return yield* buildInitialPiProviderSnapshot(settings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const models = providerModelsFromSettings([], settings.customModels, EMPTY_CAPABILITIES);
  const versionProbe = yield* Effect.gen(function* () {
    const command = settings.binaryPath || "pi";
    const resolved = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
      extendEnv: true,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(resolved.command, resolved.args, {
        ...(cwd ? { cwd } : {}),
        env: environment,
        extendEnv: true,
        shell: resolved.shell,
      }),
    );
  }).pipe(Effect.timeout("4 seconds"), Effect.result);

  if (Result.isFailure(versionProbe)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: !isCommandMissingCause(versionProbe.failure),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Could not run Pi Agent. Check its installation and binary path.",
      },
    });
  }
  const version = parseGenericCliVersion(versionProbe.success.stdout);
  if (
    versionProbe.success.code !== 0 ||
    version === null ||
    compareSemverVersions(version, "0.82.0") < 0
  ) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi Agent 0.82.0 or newer is required.",
      },
    });
  }

  const discovery = yield* runPiRpcBatch({
    binaryPath: settings.binaryPath || "pi",
    ...(cwd ? { cwd } : {}),
    environment,
    commands: [{ type: "get_state" }, { type: "get_available_models" }, { type: "get_commands" }],
  }).pipe(Effect.flatMap(decodePiDiscovery), Effect.result);

  if (Result.isFailure(discovery)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi Agent RPC discovery failed. Check the binary path and Pi version.",
      },
    });
  }
  const discovered = discovery.success;
  const thinkingLevels = new Map<string, ReadonlyArray<string>>();
  // Older Pi releases may persist model changes globally. Probe only the
  // session-local RPC implementation, with all project extensions disabled.
  if (compareSemverVersions(version, "0.85.0") >= 0) {
    yield* Effect.gen(function* () {
      const runtime = yield* makePiSessionRuntime({
        binaryPath: settings.binaryPath || "pi",
        cwd: cwd ?? process.cwd(),
        environment,
        args: ["--no-session", "--no-extensions"],
      });
      for (const model of discovered.models) {
        const levels = yield* Effect.gen(function* () {
          yield* runtime.request("set_model", { provider: model.provider, modelId: model.id });
          return yield* runtime
            .request("get_available_thinking_levels")
            .pipe(Effect.flatMap(decodePiThinkingLevelsData));
        }).pipe(Effect.result);
        if (Result.isSuccess(levels)) {
          thinkingLevels.set(`${model.provider}/${model.id}`, levels.success.levels);
        }
      }
    }).pipe(Effect.scoped, Effect.ignore);
  }
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: buildPiModels(
      discovered.models,
      discovered.state.model ?? null,
      settings.customModels,
      thinkingLevels,
    ),
    slashCommands: [
      ...buildPiSlashCommands(discovered.commands).filter((command) => command.name !== "reload"),
      { name: "reload", description: "Reload Pi extensions, skills, prompts, and context files" },
    ],
    probe: {
      installed: true,
      version,
      status: discovered.models.length > 0 ? "ready" : "warning",
      // Discovery reports configured models, not a successful authenticated API call.
      auth: { status: "unknown" },
      ...(discovered.models.length === 0
        ? { message: "Pi Agent reported no available models." }
        : compareSemverVersions(version, "0.85.0") < 0
          ? {
              message:
                "Upgrade Pi to 0.85.0 or newer for session-local reasoning and speed controls.",
            }
          : thinkingLevels.size < discovered.models.length
            ? {
                message:
                  "Some Pi model options could not be queried. Unverified controls are hidden; refresh the provider to retry.",
              }
            : {}),
    },
  });
});
