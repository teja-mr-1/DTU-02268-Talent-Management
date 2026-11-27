import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const COMMIT = "e244dacc35446986e8c4bc43080e349f7bad133d";
const BASE_URL = `https://raw.githubusercontent.com/hugoalopez-dtu/dcr-js/${COMMIT}`;
const OUT_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "src", "dcr-engine");

const FILES = [
  ["LICENSE", "LICENSE"],
  ["dcr-engine/src/types.ts", "types.ts"],
  ["dcr-engine/src/utility.ts", "utility.ts"],
  ["dcr-engine/src/guardEval.ts", "guardEval.ts"],
  ["dcr-engine/src/executionEngine.ts", "executionEngine.ts"],
  ["dcr-engine/src/conformance.ts", "conformance.ts"],
  ["dcr-engine/src/eventLogs.ts", "eventLogs.ts"],
];

async function main() {
  if (FILES.every(([, dest]) => fs.existsSync(path.join(OUT_DIR, dest)))) {
    return;
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });
  for (const [src, dest] of FILES) {
    const url = `${BASE_URL}/${src}`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Failed to fetch ${url}: ${res.status} ${res.statusText}`);
    }
    fs.writeFileSync(path.join(OUT_DIR, dest), await res.text(), "utf-8");
  }
  console.log(`Fetched dcr-engine (${FILES.length} files) from hugoalopez-dtu/dcr-js@${COMMIT.slice(0, 7)}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
