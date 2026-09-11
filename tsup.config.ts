import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["cjs", "esm"],
  // DTS emitted separately via `tsc --emitDeclarationOnly` in build script.
  // tsup's bundled dts (both dts:true and experimentalDts) is broken on Node >=26
  // because it bundles an older tsc that lacks the parseJsonConfigFileContent export.
  dts: false,
  splitting: false,
  sourcemap: true,
  clean: true,
  external: ["zeromq"],
  treeshake: true,
  target: "node18",
});
