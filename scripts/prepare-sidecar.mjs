import { execFileSync } from "node:child_process";
import { chmodSync, copyFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repository = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const targetTriple = process.env.TAURI_ENV_TARGET_TRIPLE ||
  execFileSync("rustc", ["--print", "host-tuple"], { encoding: "utf8" }).trim();
const debug = process.env.TAURI_ENV_DEBUG === "true";
const profile = debug ? "debug" : "release";
const profileArgs = debug ? [] : ["--release"];
const extension = targetTriple.includes("windows") ? ".exe" : "";
const targetArgs = process.env.TAURI_ENV_TARGET_TRIPLE ? ["--target", targetTriple] : [];
const source = join(
  repository,
  "target",
  ...(targetArgs.length ? [targetTriple] : []),
  profile,
  `bastet-daemon${extension}`,
);
const destination = join(
  repository,
  "apps",
  "desktop",
  "src-tauri",
  "binaries",
  `bastet-daemon-${targetTriple}${extension}`,
);

execFileSync("cargo", ["build", "-p", "bastet-daemon", ...profileArgs, "--locked", ...targetArgs], {
  cwd: repository,
  stdio: "inherit",
});
mkdirSync(dirname(destination), { recursive: true });
copyFileSync(source, destination);
if (!extension) chmodSync(destination, 0o755);
console.log(`Prepared Bastet daemon sidecar: ${destination}`);
