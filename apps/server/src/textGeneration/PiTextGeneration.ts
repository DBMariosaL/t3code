import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  ThreadId,
  TextGenerationError,
  type PiAgentSettings,
  type ModelSelection,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { makePiAdapter } from "../provider/pi/PiAdapter.ts";

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  settings: PiAgentSettings,
  instanceId: ProviderInstanceId,
  environment: NodeJS.ProcessEnv = process.env,
  commandPrefix: ReadonlyArray<string> = [],
) {
  const crypto = yield* Crypto.Crypto;
  const adapter = yield* makePiAdapter(settings, {
    instanceId,
    environment,
    commandPrefix: [
      ...commandPrefix,
      "--no-tools",
      "--no-extensions",
      "--no-skills",
      "--no-prompt-templates",
      "--no-session",
    ],
  });
  const runPiJson = <S extends Schema.Top>(input: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const threadId = ThreadId.make(yield* crypto.randomUUIDv4);
      const done = yield* Deferred.make<string, TextGenerationError>();
      let output = "";
      yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.gen(function* () {
          if (event.threadId !== threadId) return;
          if (event.type === "content.delta" && event.payload.streamKind === "assistant_text") {
            output += event.payload.delta;
            if (output.length > 1024 * 1024) {
              yield* Deferred.fail(
                done,
                new TextGenerationError({
                  operation: input.operation,
                  detail: "Pi text generation exceeded the output limit.",
                }),
              );
            }
          }
          if (event.type === "turn.completed") {
            if (event.payload.state === "completed") yield* Deferred.succeed(done, output);
            else
              yield* Deferred.fail(
                done,
                new TextGenerationError({
                  operation: input.operation,
                  detail: event.payload.errorMessage ?? "Pi text generation did not complete.",
                }),
              );
          }
          if (event.type === "session.exited")
            yield* Deferred.fail(
              done,
              new TextGenerationError({
                operation: input.operation,
                detail: "Pi exited during text generation.",
              }),
            );
        }),
      ).pipe(Effect.forkChild);
      return yield* Effect.gen(function* () {
        yield* adapter.startSession({
          threadId,
          cwd: input.cwd,
          runtimeMode: "full-access",
          modelSelection: input.modelSelection,
        });
        yield* adapter.sendTurn({ threadId, input: input.prompt });
        const text = yield* Deferred.await(done);
        const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(input.outputSchemaJson));
        return yield* decodeOutput(extractJsonObject(text));
      }).pipe(Effect.ensuring(adapter.stopSession(threadId).pipe(Effect.ignore)));
    }).pipe(
      Effect.timeout("180 seconds"),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({
            operation: input.operation,
            detail: "Pi structured text generation failed.",
            cause,
          }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
