import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const tsc = path.join(
  packageRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc",
);

function npm(args) {
  const command = process.env.npm_execpath
    ? [process.execPath, process.env.npm_execpath]
    : [process.platform === "win32" ? "npm.cmd" : "npm"];
  const result = spawnSync(command[0], [...command.slice(1), ...args], {
    cwd: packageRoot,
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

if (!existsSync(tsc)) npm(["ci", "--ignore-scripts"]);
npm(["run", "build"]);
