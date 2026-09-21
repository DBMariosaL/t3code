/** Written to a scope-owned file and loaded by Pi through --extension. */
export const PI_BRIDGE_SOURCE = `import { readFileSync } from "node:fs";
export default function (pi) {
  pi.on("before_provider_request", (event, ctx) => {
    const { serviceTier } = JSON.parse(readFileSync(new URL("./options.json", import.meta.url), "utf8"));
    if (serviceTier === undefined) return;
    const supported = ["openai-responses", "openai-codex-responses"].includes(ctx.model?.api);
    if (!supported) {
      if (serviceTier === "priority") throw new Error("Pi Fast mode is unavailable for this model API.");
      return;
    }
    const { service_tier: _previous, ...payload } = event.payload;
    return serviceTier === "priority" ? { ...payload, service_tier: "priority" } : payload;
  });
  pi.registerCommand("t3-reload", {
    description: "Reload Pi extensions, skills, prompts, and context files",
    handler: async (_args, ctx) => {
      await ctx.reload();
      return;
    },
  });
}
`;
