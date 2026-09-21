import * as NodeReadline from "node:readline";

const input = NodeReadline.createInterface({ input: process.stdin, crlfDelay: Infinity });
const commands = [];
input.on("line", (line) => {
  commands.push(JSON.parse(line));
  if (commands.length !== 2) return;
  const replies = commands.toReversed().map((command) => ({
    type: "response",
    id: command.id,
    command: command.type,
    success: true,
    data: { label: "Modèle π" },
  }));
  const bytes = Buffer.from(replies.map((reply) => JSON.stringify(reply)).join("\n") + "\n");
  const split = bytes.indexOf(Buffer.from("π")) + 1;
  process.stdout.write(bytes.subarray(0, split), () => {
    setImmediate(() => process.stdout.write(bytes.subarray(split)));
  });
});
