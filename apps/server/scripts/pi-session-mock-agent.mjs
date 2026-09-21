import * as NodeReadline from "node:readline";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const args = process.argv.slice(2);
if (process.env.T3_PI_MOCK_SPAWN_LOG)
  NodeFS.writeFileSync(process.env.T3_PI_MOCK_SPAWN_LOG, JSON.stringify(args));
const arg = (name) => args[args.indexOf(name) + 1];
const sessionFile = args.includes("--session")
  ? arg("--session")
  : NodePath.join(process.cwd(), "mock-pi-session.json");
let messages = NodeFS.existsSync(sessionFile)
  ? JSON.parse(NodeFS.readFileSync(sessionFile, "utf8"))
  : [];
let streaming = false;
let model = { provider: "mock", id: "model" };
let thinkingLevel = "medium";
let reloads = 0;
const commands = new Map();
const extension = await import(NodeURL.pathToFileURL(arg("--extension")).href);
extension.default({
  registerCommand: (name, command) => commands.set(name, command),
  on: () => {},
});
const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
const reply = (request, data) =>
  send({ type: "response", command: request.type, id: request.id, success: true, data });
const save = () => NodeFS.writeFileSync(sessionFile, JSON.stringify(messages));
const finish = (text = "Pi answer", stopReason = "stop") => {
  const message = { role: "assistant", content: [{ type: "text", text }], stopReason };
  send({ type: "message_start", message: { role: "assistant", content: [] } });
  send({
    type: "message_update",
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text },
  });
  send({
    type: "tool_execution_start",
    toolCallId: "tool-1",
    toolName: "bash",
    args: { command: "echo hello" },
  });
  send({
    type: "tool_execution_end",
    toolCallId: "tool-1",
    toolName: "bash",
    result: { content: [{ type: "text", text: "hello" }] },
    isError: false,
  });
  send({ type: "message_end", message });
  messages.push(message);
  save();
  send({ type: "agent_end", messages: [message] });
  streaming = false;
  send({ type: "agent_settled" });
};
const input = NodeReadline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on("line", async (line) => {
  const request = JSON.parse(line);
  if (process.env.T3_PI_MOCK_LOG) NodeFS.appendFileSync(process.env.T3_PI_MOCK_LOG, line + "\n");
  switch (request.type) {
    case "get_state":
      reply(request, {
        model,
        thinkingLevel,
        sessionFile,
        sessionId: "mock-session",
        isStreaming: streaming,
        isCompacting: false,
        pendingMessageCount: 0,
        reloads,
      });
      break;
    case "get_commands":
      reply(request, { commands: [...commands.keys()].map((name) => ({ name })) });
      break;
    case "set_model":
      model = { provider: request.provider, id: request.modelId };
      thinkingLevel = "medium";
      reply(request, model);
      break;
    case "get_available_thinking_levels":
      reply(request, { levels: ["off", "low", "medium", "high"] });
      break;
    case "set_thinking_level":
      thinkingLevel = process.env.T3_PI_MOCK_CLAMP ? "low" : request.level;
      reply(request);
      break;
    case "get_messages":
      reply(request, { messages });
      break;
    case "get_fork_messages":
      reply(request, {
        messages: messages.flatMap((message, index) =>
          message.role === "user" ? [{ entryId: String(index), text: message.content }] : [],
        ),
      });
      break;
    case "fork":
      messages = messages.slice(0, Number(request.entryId));
      save();
      reply(request, { cancelled: false });
      break;
    case "abort":
      finish("Interrupted", "aborted");
      reply(request);
      break;
    case "extension_ui_response":
      finish(String(request.value ?? request.confirmed));
      break;
    case "prompt": {
      if (request.message === "/t3-reload") {
        try {
          await commands.get("t3-reload").handler("", {
            reload: async () => {
              if (process.env.T3_PI_MOCK_FAIL_RELOAD) throw new Error("Reload failed");
              reloads++;
            },
          });
        } catch (error) {
          send({ type: "extension_error", error: error.message });
        }
        reply(request);
        break;
      }
      if (request.message === "reject") {
        send({
          type: "response",
          command: "prompt",
          id: request.id,
          success: false,
          error: "Prompt rejected",
        });
        break;
      }
      if (request.message === "crash") {
        process.exit(7);
      }
      messages.push({ role: "user", content: request.message });
      save();
      streaming = true;
      reply(request);
      send({ type: "agent_start" });
      if (request.message === "hold") {
        send({ type: "agent_end", messages: [], willRetry: true });
        break;
      }
      if (request.message === "ask") {
        send({
          type: "extension_ui_request",
          id: "question-1",
          method: "select",
          title: "Choose one",
          options: ["A", "B"],
        });
        break;
      }
      finish(process.env.T3_PI_MOCK_TEXT ?? request.message);
      break;
    }
    default:
      send({
        type: "response",
        command: request.type,
        id: request.id,
        success: false,
        error: "Unknown command",
      });
  }
});
