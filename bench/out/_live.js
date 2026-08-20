
const b = document.getElementById('b');
const clk = document.getElementById('clk');
const cur = document.createElement('span'); cur.className='cursor';
function add(text, cls){
  const d = document.createElement('div');
  d.className = 'l ' + (cls||'');
  d.textContent = text;
  b.insertBefore(d, cur);
  while (b.scrollHeight > b.clientHeight && b.firstChild !== cur) b.removeChild(b.firstChild);
}
add('PS C:\\Users\\Ilay\\test\\revscope> python -u -m bench.run_all', 'cmd');
b.appendChild(cur);
function cls(s){
  if (/^processed /.test(s)) return 'bulk';
  if (/^\[\d\/7\]/.test(s)) return 'step';
  if (/PASS/.test(s)) return 'pass';
  if (/^bench complete/.test(s)) return 'done';
  return '';
}
let shown = 0;
const t0 = Date.now();
setInterval(() => {
  const s = Math.floor((Date.now()-t0)/1000);
  clk.textContent = String(Math.floor(s/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0');
}, 500);
async function poll(){
  try{
    const r = await fetch('live.log?ts=' + Date.now(), {cache:'no-store'});
    const txt = await r.text();
    const lines = txt.split('\n');
    for (let i = shown; i < lines.length - 1; i++) add(lines[i], cls(lines[i]));
    if (lines.length - 1 > shown) shown = lines.length - 1;
    if (/bench complete/.test(txt)) window.__finished = true;
  }catch(e){}
  setTimeout(poll, 250);
}
poll();
