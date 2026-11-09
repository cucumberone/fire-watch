/**
 * Build with esbuild — no bundler config to keep correct, no framework CLI.
 *
 * Produces:
 *   dist/index.html      shell (no-store, so a deploy is picked up immediately)
 *   dist/app.[hash].js   the bundle (immutable cache)
 *   dist/app.css
 *
 * config.json is NOT built here: it is mounted from a ConfigMap at runtime so
 * one image works in every environment (docs/frontend.md §3).
 */
import { build, context } from 'esbuild';
import { mkdir, writeFile, rm } from 'node:fs/promises';
import { createHash } from 'node:crypto';

const watch = process.argv.includes('--watch');

const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Fire-Watch</title>
<link rel="stylesheet" href="/app.css" />
</head>
<body>
<div id="root"></div>
<script type="module" src="/app.js"></script>
</body>
</html>
`;

const css = `
:root { color-scheme: light dark; --fg:#1a1a1a; --dim:#777; --line:#e3e3e3; --bg:#fff; }
@media (prefers-color-scheme: dark) { :root { --fg:#e8e8e8; --dim:#999; --line:#333; --bg:#141414; } }
* { box-sizing: border-box; }
body { margin:0; font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
       background:var(--bg); color:var(--fg); }
main { max-width: 1200px; margin: 0 auto; padding: 1.5rem; }
h1 { font-size:1.25rem; margin:0; }
h2 { font-size:1rem; margin:2rem 0 .5rem; font-weight:600; }
header { display:flex; gap:1rem; align-items:center; flex-wrap:wrap;
         border-bottom:1px solid var(--line); padding-bottom:.75rem; }
table { width:100%; border-collapse:collapse; font-size:.85rem; }
th { text-align:left; font-weight:600; color:var(--dim); border-bottom:1px solid var(--line);
     padding:.4rem .5rem; position:sticky; top:0; background:var(--bg); }
td { padding:.4rem .5rem; border-bottom:1px solid var(--line); vertical-align:top; }
tr.retirement td { opacity:.55; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:.8rem; }
.dim { color:var(--dim); }
.num { text-align:right; font-variant-numeric: tabular-nums; }
.sev { display:inline-block; padding:.1rem .4rem; border-radius:.25rem; font-size:.7rem;
       font-weight:700; letter-spacing:.03em; }
.sev-fatal    { background:#7f1d1d; color:#fff; }
.sev-critical { background:#dc2626; color:#fff; }
.sev-warning  { background:#f59e0b; color:#1a1a1a; }
.sev-harmless { background:#e5e7eb; color:#374151; }
.sev-close    { background:#e5e7eb; color:#374151; }
.status { font-size:.75rem; padding:.1rem .5rem; border-radius:999px; background:#e5e7eb; color:#374151; }
.status-open { background:#16a34a; color:#fff; }
.status-reconnecting, .status-connecting { background:#f59e0b; color:#1a1a1a; }
.status-misconfigured { background:#dc2626; color:#fff; }
.notice { font-size:.8rem; color:#b45309; }
.error { color:#dc2626; }
.link { background:none; border:0; color:#2563eb; cursor:pointer; font-size:.75rem;
        text-decoration:underline; margin-left:.5rem; padding:0; }
pre.detail { margin:.25rem 0 0; padding:.6rem; background:rgba(127,127,127,.12);
             border-radius:.25rem; font-size:.75rem; overflow-x:auto; white-space:pre-wrap; }
`;

async function main() {
  await rm('dist', { recursive: true, force: true });
  await mkdir('dist', { recursive: true });

  const result = await build({
    entryPoints: ['src/app.jsx'],
    bundle: true,
    minify: !watch,
    sourcemap: watch,
    format: 'esm',
    target: ['es2020'],
    outfile: 'dist/app.js',
    jsx: 'automatic',
    loader: { '.js': 'jsx' },
    define: { 'process.env.NODE_ENV': watch ? '"development"' : '"production"' },
    logLevel: 'info',
    metafile: true,
  });

  // Fingerprint the bundle so assets can be cached immutably while index.html
  // stays no-store. Without this a deploy can serve a stale bundle next to a
  // fresh shell.
  const { readFile } = await import('node:fs/promises');
  const js = await readFile('dist/app.js');
  const hash = createHash('sha256').update(js).digest('hex').slice(0, 12);
  await writeFile(`dist/app.${hash}.js`, js);
  await rm('dist/app.js');
  await writeFile('dist/app.css', css.trim() + '\n');
  await writeFile(
    'dist/index.html',
    html.replace('/app.js', `/app.${hash}.js`),
  );

  if (Object.keys(result.metafile?.outputs || {}).length === 0) {
    throw new Error('esbuild produced no output');
  }
  console.log(`built dist/index.html + dist/app.${hash}.js`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
