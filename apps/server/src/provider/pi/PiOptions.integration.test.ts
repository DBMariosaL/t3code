// @effect-diagnostics nodeBuiltinImport:off
import * as NodeHttp from "node:http";
import * as NodeZlib from "node:zlib";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  PiAgentSettings,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { makePiAdapter } from "./PiAdapter.ts";
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeSettings = Schema.decodeUnknownEffect(PiAgentSettings);

// Opt in with the installed Pi CLI JS entry point. All requests stay on loopback;
// credentials, settings and transcripts belong to a disposable test directory.
describe.skipIf(!process.env.T3_PI_TEST_CLI)("installed Pi model options", () => {
  for (const api of ["openai-responses", "openai-codex-responses"]) {
    it.live(
      `sends and clears priority through ${api}, including reload and resume`,
      () =>
        Effect.gen(function* () {
          const requests: Record<string, unknown>[] = [];
          let refusedPriority = false;
          const server = NodeHttp.createServer(async (request, response) => {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const bytes = Buffer.concat(chunks);
            const body =
              request.headers["content-encoding"] === "zstd"
                ? NodeZlib.zstdDecompressSync(bytes)
                : bytes;
            requests.push(JSON.parse(body.toString("utf8")));
            if (requests.at(-1)?.service_tier === "priority" && !refusedPriority) {
              refusedPriority = true;
              response.writeHead(400, { "content-type": "application/json" });
              response.end(
                JSON.stringify({
                  error: {
                    message: "Fixture rejected service tier",
                    type: "invalid_request_error",
                  },
                }),
              );
              return;
            }
            response.writeHead(200, { "content-type": "text/event-stream" });
            const item = {
              id: "message-fixture",
              type: "message",
              role: "assistant",
              status: "completed",
              content: [{ type: "output_text", text: "OK", annotations: [] }],
            };
            for (const event of [
              { type: "response.created", response: { id: "response-fixture", output: [] } },
              {
                type: "response.output_item.added",
                output_index: 0,
                item: { ...item, content: [] },
              },
              {
                type: "response.content_part.added",
                output_index: 0,
                content_index: 0,
                part: { type: "output_text", text: "", annotations: [] },
              },
              {
                type: "response.output_text.delta",
                output_index: 0,
                content_index: 0,
                delta: "OK",
              },
              { type: "response.output_item.done", output_index: 0, item },
              {
                type: "response.completed",
                response: {
                  id: "response-fixture",
                  status: "completed",
                  output: [item],
                  usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 },
                },
              },
            ])
              response.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
            response.end();
          });
          yield* Effect.acquireRelease(
            Effect.promise(
              () => new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve)),
            ),
            () =>
              Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
          );
          const address = server.address();
          assert.isNotNull(address);
          if (typeof address !== "object" || !address) throw new Error("Missing fixture port");
          const baseUrl = `http://127.0.0.1:${address.port}/v1`;
          const fs = yield* FileSystem.FileSystem;
          const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-real-options-" });
          const token = `test.${Buffer.from(yield* encodeJson({ "https://api.openai.com/auth": { chatgpt_account_id: "fixture" } })).toString("base64")}.test`;
          yield* fs.writeFileString(
            `${cwd}/models.json`,
            yield* encodeJson({
              providers: {
                "t3-fixture": {
                  baseUrl,
                  api,
                  apiKey: token,
                  models: [{ id: "gpt-5.4", reasoning: true }],
                },
              },
            }),
          );
          const originalSettings = yield* encodeJson({
            defaultProvider: "t3-fixture",
            defaultModel: "gpt-5.4",
            defaultThinkingLevel: "medium",
            transport: "sse",
            retry: { enabled: false },
          });
          yield* fs.writeFileString(`${cwd}/settings.json`, originalSettings);
          const adapter = yield* makePiAdapter(
            yield* decodeSettings({ enabled: true, binaryPath: process.execPath }),
            {
              commandPrefix: [
                process.env.T3_PI_TEST_CLI!,
                "--no-extensions",
                "--no-skills",
                "--no-prompt-templates",
              ],
              environment: { ...process.env, PI_CODING_AGENT_DIR: cwd },
            },
          );
          const completed = yield* Queue.unbounded<ProviderRuntimeEvent>();
          yield* Stream.runForEach(adapter.streamEvents, (event) =>
            event.type === "turn.completed" ? Queue.offer(completed, event) : Effect.void,
          ).pipe(Effect.forkChild);
          const threadId = ThreadId.make("real-pi-options");
          const selection = (tier: string) => ({
            instanceId: ProviderInstanceId.make("piAgent"),
            model: "t3-fixture/gpt-5.4",
            options: [
              { id: "reasoningEffort", value: "high" },
              { id: "serviceTier", value: tier },
            ],
          });
          yield* adapter.startSession({
            threadId,
            cwd,
            runtimeMode: "full-access",
          });
          yield* adapter.sendTurn({ threadId, input: "Keep Pi defaults." });
          const initial = yield* Queue.take(completed);
          if (initial.type === "turn.completed") assert.equal(initial.payload.state, "completed");
          assert.isUndefined(requests[0]?.service_tier);
          assert.equal((requests[0]?.reasoning as Record<string, unknown>)?.effort, "medium");
          const first = yield* adapter.sendTurn({
            threadId,
            input: "Reply briefly.",
            modelSelection: selection("priority"),
          });
          const failed = yield* Queue.take(completed);
          assert.equal(failed.type, "turn.completed");
          if (failed.type === "turn.completed") assert.equal(failed.payload.state, "failed");
          assert.equal(requests.at(-1)?.service_tier, "priority");
          assert.deepStrictEqual(
            (requests.at(-1)?.reasoning as Record<string, unknown>)?.effort,
            "high",
          );
          yield* adapter.sendTurn({ threadId, input: "/reload" });
          yield* Queue.take(completed);
          yield* adapter.sendTurn({ threadId, input: "After reload." });
          const reloaded = yield* Queue.take(completed);
          if (reloaded.type === "turn.completed") assert.equal(reloaded.payload.state, "completed");
          assert.equal(requests.at(-1)?.service_tier, "priority");
          yield* adapter.stopSession(threadId);
          yield* adapter.startSession({
            threadId,
            cwd,
            runtimeMode: "full-access",
            resumeCursor: first.resumeCursor,
            modelSelection: selection("priority"),
          });
          yield* adapter.sendTurn({ threadId, input: "Reply again." });
          const resumed = yield* Queue.take(completed);
          if (resumed.type === "turn.completed") assert.equal(resumed.payload.state, "completed");
          assert.equal(requests.at(-1)?.service_tier, "priority");
          yield* adapter.sendTurn({
            threadId,
            input: "Normal request.",
            modelSelection: selection("default"),
          });
          const normal = yield* Queue.take(completed);
          if (normal.type === "turn.completed") assert.equal(normal.payload.state, "completed");
          assert.isUndefined(requests.at(-1)?.service_tier);
          assert.equal(yield* fs.readFileString(`${cwd}/settings.json`), originalSettings);
          yield* adapter.stopAll();
        }).pipe(Effect.timeout("20 seconds"), Effect.scoped, Effect.provide(NodeServices.layer)),
      { timeout: 30_000 },
    );
  }
});
