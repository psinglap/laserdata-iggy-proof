"""Builds index.html: a standalone page with model.js and the calculator inlined."""
model = open('model.js').read()
frag = open('calc.fragment.html').read()
open('index.html', 'w').write(f'''<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Apache Iggy TCO Calculator</title>
<style>
:root{{--bg:#f5f6f8;--paper:#fff;--ink:#141a26;--muted:#5d6779;--line:#dde2ea;--lime:#4f7f1a;--lime-fill:#7fae3c;--lime-soft:#eef5e4;--rose:#c0392b;--rose-soft:#fbe9ea;--serif:Georgia,serif;--mono:ui-monospace,Menlo,monospace}}
@media (prefers-color-scheme:dark){{:root{{--bg:#0f141c;--paper:#171e2a;--ink:#e6ebf3;--muted:#9aa5b8;--line:#2a3445;--lime:#a6d36a;--lime-fill:#7fae3c;--lime-soft:#1f2b17;--rose:#e57368;--rose-soft:#3a1f1f;color-scheme:dark}}}}
body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,sans-serif}}
main{{max-width:980px;margin:0 auto;padding:28px max(16px,env(safe-area-inset-left,0px)) 48px}}
h1{{font:400 1.9rem/1.2 var(--serif);margin:0 0 6px}}p.lead{{color:var(--muted);margin:0 0 20px}}
.svg-s{{font:400 10px var(--mono);fill:var(--muted)}}a{{color:var(--lime)}}
</style>
<main><h1>Apache Iggy TCO calculator</h1><p class="lead">From LaserData. Every input is visible and editable; nothing is multiplied by a number you can't see.</p>
<script>{model}</script>
{frag}
</main>
''')
