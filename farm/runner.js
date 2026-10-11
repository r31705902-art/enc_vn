#!/usr/bin/env node
'use strict';
// V_N farm runner — выполняет ОДНУ задачу на GitHub Actions и выходит.
//   TASK=a   — probe + план + аудио + storyboard + постер
//   TASK=<n> — видео-кусок n (все качества лесенки)
//   авто-нарезка (farm/autocut.js): d — скачать по ссылке, t<k> — речь, s — субтитры + нейросеть, i — вступление, p<k> — собрать часть
// Ничего секретного в этом файле нет: адрес диспетчера и ключ приходят из GitHub Secrets,
// всё остальное (хранилище, задача) выдаёт диспетчер по одноразовому тикету.
const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { spawn, execFileSync, execFile } = require('node:child_process');
const execA = (cmd, args) => new Promise((res, rej) => execFile(cmd, args, { encoding: 'utf8', maxBuffer: 32 << 20 }, (e, out, err) => (e ? rej(new Error(String(err || e.message).slice(-800))) : res(out))));
const kids = new Set();

const FARM_URL = String(process.env.FARM_URL || '').replace(/\/$/, '');
const FARM_KEY = process.env.FARM_KEY || '';
const TICKET = process.env.TICKET || '';
const TASK = String(process.env.TASK || '');
const FFMPEG = process.env.FFMPEG || 'ffmpeg', FFPROBE = process.env.FFPROBE || 'ffprobe';
const WORK = process.env.WORK_DIR || path.join(require('node:os').tmpdir(), 'farm');
const GH = !!process.env.GITHUB_ACTIONS;

if (!FARM_URL || !FARM_KEY || !TICKET || !TASK) { console.error('нет FARM_URL / FARM_KEY / TICKET / TASK'); process.exit(2); }
const masked = new Set();
const mask = (s) => { s = String(s || ''); if (s.length < 4 || masked.has(s)) return; masked.add(s); if (GH) console.log('::add-mask::' + s); };
mask(FARM_URL); mask(new URL(FARM_URL).host); mask(TICKET);
// любые строки в логах проходят через scrub — на всякий случай, поверх маскировки GitHub
const scrub = (t) => { t = String(t); for (const s of masked) t = t.split(s).join('***'); return t; };
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), scrub(a.map((x) => (typeof x === 'string' ? x : JSON.stringify(x))).join(' ')));

// ---------- dispatcher api ----------
async function api(p, body, { method, raw, tries = 6 } = {}) {
  for (let i = 0; ; i++) {
    try {
      const r = await fetch(FARM_URL + p, {
        method: method || (body === undefined ? 'GET' : 'POST'),
        headers: { Authorization: 'Bearer ' + FARM_KEY, 'X-Ticket': TICKET, 'X-Task': TASK, 'Content-Type': raw ? 'application/octet-stream' : 'application/json' },
        body: body === undefined ? undefined : raw ? body : JSON.stringify(body), signal: AbortSignal.timeout(60000),
      });
      const j = await r.json().catch(() => ({}));
      if (r.status === 401 || r.status === 404 || r.status === 410) { const e = new Error('dispatcher: ' + r.status + ' ' + (j.error || '')); e.fatal = true; throw e; }
      if (!r.ok) throw new Error('dispatcher: HTTP ' + r.status + ' ' + (j.error || ''));
      return j;
    } catch (e) { if (e.fatal || i >= tries - 1) throw e; await sleep(1500 * (i + 1)); }
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- minimal Swift client (borrowed token) ----------
const agents = { 'http:': new http.Agent({ keepAlive: true, maxSockets: 32 }), 'https:': new https.Agent({ keepAlive: true, maxSockets: 32 }) };
const S = { url: '', token: '', exp: 0, container: '' };
const setCreds = (c) => { Object.assign(S, { url: c.url, token: c.token, exp: c.exp || 0, container: c.container }); mask(c.token); mask(c.url); };
async function refreshCreds() { setCreds(await api('/w/storage')); }
const objUrl = (name) => S.url + '/' + encodeURIComponent(S.container) + '/' + name.split('/').map(encodeURIComponent).join('/');
function sreq(method, name, headers = {}, body, timeout = 120000) {
  const u = new URL(objUrl(name));
  return new Promise((resolve, reject) => {
    const lib = u.protocol === 'https:' ? https : http;
    const rq = lib.request(u, { method, agent: agents[u.protocol], headers: { 'X-Auth-Token': S.token, ...headers } }, resolve);
    rq.setTimeout(timeout, () => rq.destroy(new Error('storage timeout')));
    rq.on('error', reject); rq.end(body);
  });
}
async function sget(name, range) {
  for (let i = 0; ; i++) {
    try {
      if (S.exp && Date.now() > S.exp - 10 * 60e3) await refreshCreds();
      const res = await sreq('GET', name, range ? { Range: range } : {});
      if (res.statusCode === 401) { res.resume(); await refreshCreds(); throw new Error('storage 401'); }
      if (res.statusCode >= 300) { res.resume(); throw new Error('storage GET ' + res.statusCode); }
      const chunks = []; for await (const c of res) chunks.push(c);
      return Buffer.concat(chunks);
    } catch (e) { if (i >= 5) throw e; await sleep(1000 * 2 ** i); }
  }
}
// размер файла в хранилище; -1 = файла нет. 5xx/сеть — повторяем (503 ≠ «файл пропал»)
async function shead(name) {
  for (let i = 0; ; i++) {
    try {
      const res = await sreq('HEAD', name); res.resume();
      if (res.statusCode === 401) { await refreshCreds(); throw new Error('storage 401'); }
      if (res.statusCode === 404) return -1;
      if (res.statusCode >= 300) throw new Error('storage HEAD ' + res.statusCode);
      return Number(res.headers['content-length'] || 0);
    } catch (e) { if (i >= 7) throw e; await sleep(Math.min(30000, 1000 * 2 ** i) * (0.75 + Math.random() * 0.5)); }
  }
}
async function sput(name, buf, type) {
  for (let i = 0; ; i++) {
    try {
      if (S.exp && Date.now() > S.exp - 10 * 60e3) await refreshCreds();
      const res = await sreq('PUT', name, { 'Content-Type': type, 'Content-Length': buf.length }, buf, 600000); res.resume();
      if (res.statusCode === 401) { await refreshCreds(); throw new Error('storage 401'); }
      if (res.statusCode >= 300) throw new Error('storage PUT ' + res.statusCode);
      return;
    } catch (e) { // Blomp под нагрузкой отвечает 503 минутами — ждём дольше (до ~5 мин суммарно)
      if (i >= 11) throw e;
      if (i >= 3) log(`хранилище: ${e.message}, повтор ${i + 1}…`);
      await sleep(Math.min(45000, 1500 * 2 ** i) * (0.75 + Math.random() * 0.5));
    }
  }
}
function pool(n) {
  const q = []; let active = 0, err = null; const idle = [];
  const next = () => { while (active < n && q.length) { const fn = q.shift(); active++; fn().catch((e) => { err = err || e; }).finally(() => { active--; next(); if (!active && !q.length) idle.splice(0).forEach((f) => f()); }); } };
  return { add: (fn) => { q.push(fn); next(); }, drain: () => new Promise((r) => (!active && !q.length ? r() : idle.push(r))), get err() { return err; } };
}

// ---------- local range proxy: ffmpeg читает исходник по HTTP, мы тянем только нужные части из хранилища ----------
async function sourceProxy(job) {
  const parts = job.rawParts, size = job.rawSize;
  const pname = (i) => `raw/${job.videoId}/p_${String(i).padStart(5, '0')}`;
  const partSize = parts > 1 ? await shead(pname(1)) : size;
  if (!(partSize > 0)) throw new Error('raw part size = ' + partSize);
  const server = http.createServer(async (req, res) => {
    const m = /^bytes=(\d+)-(\d*)$/.exec(req.headers.range || '');
    let a = m ? +m[1] : 0, b = m && m[2] ? Math.min(+m[2], size - 1) : size - 1;
    if (a >= size) { res.writeHead(416, { 'Content-Range': `bytes */${size}` }); return res.end(); }
    res.writeHead(m ? 206 : 200, { 'Accept-Ranges': 'bytes', 'Content-Length': b - a + 1, ...(m ? { 'Content-Range': `bytes ${a}-${b}/${size}` } : {}), 'Content-Type': 'application/octet-stream' });
    if (req.method === 'HEAD') return res.end();
    let closed = false; res.on('close', () => { closed = true; });
    const first = Math.floor(a / partSize) + 1, last = Math.floor(b / partSize) + 1;
    const fetchPart = (i) => {
      const ps = (i - 1) * partSize, from = Math.max(a, ps) - ps, to = Math.min(b, ps + partSize - 1) - ps;
      const whole = from === 0 && to === Math.min(partSize, size - ps) - 1;
      return sget(pname(i), whole ? undefined : `bytes=${from}-${to}`);
    };
    const ahead = new Map(); const AHEAD = 3;
    try {
      for (let i = first; i <= last && !closed; i++) {
        for (let k = i; k <= Math.min(last, i + AHEAD); k++) if (!ahead.has(k)) { const p = fetchPart(k); p.catch(() => {}); ahead.set(k, p); }
        const buf = await ahead.get(i); ahead.delete(i);
        if (closed) break;
        if (!res.write(buf)) await new Promise((r) => { const done = () => { res.off('drain', done); res.off('close', done); r(); }; res.on('drain', done); res.on('close', done); });
      }
      res.end();
    } catch (e) { log('source proxy error:', e.message); res.destroy(); }
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return { url: `http://127.0.0.1:${server.address().port}/src`, close: () => server.close() };
}

// ---------- ffmpeg helpers ----------
function runFF(args, onProgress) {
  return new Promise((resolve, reject) => {
    const ff = spawn(FFMPEG, ['-hide_banner', '-nostdin', '-y', '-loglevel', 'error', '-progress', 'pipe:1', '-nostats', ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let err = '', prog = {};
    ff.stderr.on('data', (d) => { err = (err + d).slice(-3000); });
    ff.stdout.on('data', (d) => { for (const l of String(d).split('\n')) { const [k, v] = l.split('='); if (k && v !== undefined) prog[k.trim()] = v.trim(); } if (onProgress && prog.progress) onProgress(prog); });
    kids.add(ff);
    ff.on('close', (c) => kids.delete(ff));
    ff.on('close', (c) => (c === 0 ? resolve() : reject(new Error('ffmpeg exit ' + c + ': ' + err.slice(-1200)))));
  });
}
function codecString(buf) {
  let i = buf.indexOf(Buffer.from('avcC'));
  if (i > 0) { const b = buf.subarray(i + 4, i + 8); return 'avc1.' + [b[1], b[2], b[3]].map((x) => x.toString(16).padStart(2, '0')).join(''); }
  i = buf.indexOf(Buffer.from('av1C'));
  if (i > 0) {
    const b1 = buf[i + 5], b2 = buf[i + 6];
    const prof = b1 >> 5, lvl = b1 & 31, tier = (b2 >> 7) & 1, hbd = (b2 >> 6) & 1, tw = (b2 >> 5) & 1;
    return `av01.${prof}.${String(lvl).padStart(2, '0')}${tier ? 'H' : 'M'}.${String(hbd ? (tw ? 12 : 10) : 8).padStart(2, '0')}`;
  }
  return 'avc1.640028';
}
// movenc всегда начинает tfdt с 0 → сдвигаем на начало куска, чтобы куски склеивались в одну непрерывную ленту времени
function shiftFrag(buf, ticks) {
  if (!ticks) return buf;
  const add64 = (o) => { const v = buf.readBigUInt64BE(o) + BigInt(ticks); buf.writeBigUInt64BE(v, o); };
  const walk = (start, end, inMoof) => {
    for (let o = start; o + 8 <= end;) {
      let size = buf.readUInt32BE(o); const type = buf.toString('latin1', o + 4, o + 8); let hdr = 8;
      if (size === 1) { size = Number(buf.readBigUInt64BE(o + 8)); hdr = 16; } else if (size === 0) size = end - o;
      if (size < hdr || o + size > end) break;
      if (type === 'moof' || type === 'traf') walk(o + hdr, o + size, true);
      else if (type === 'tfdt' && inMoof) { const v = buf[o + hdr]; if (v === 1) add64(o + hdr + 4); else { const n = buf.readUInt32BE(o + hdr + 4) + ticks; if (n > 0xffffffff) throw new Error('tfdt v0 overflow'); buf.writeUInt32BE(n, o + hdr + 4); } }
      else if (type === 'sidx') { const v = buf[o + hdr]; if (v === 1) add64(o + hdr + 12); else buf.writeUInt32BE(buf.readUInt32BE(o + hdr + 12) + ticks, o + hdr + 12); }
      o += size;
    }
  };
  walk(0, buf.length, false);
  return buf;
}
function mdhdTimescale(init) {
  const i = init.indexOf(Buffer.from('mdhd')); if (i < 0) throw new Error('нет mdhd');
  return init.readUInt32BE(i + 4 + (init[i + 4] === 1 ? 20 : 12));
}
function parsePlaylist(txt) {
  const out = []; let d = 0;
  for (const l of txt.split('\n')) { if (l.startsWith('#EXTINF:')) d = parseFloat(l.slice(8)); else if (l && !l.startsWith('#')) out.push([path.basename(l.trim()), d]); }
  return out;
}
const snapFps = (num, den) => {
  const f = num / den;
  for (const [n, d] of [[24000, 1001], [24, 1], [25, 1], [30000, 1001], [30, 1], [50, 1], [60000, 1001], [60, 1], [120, 1], [144, 1]]) if (Math.abs(f - n / d) < 0.02) return [n, d];
  return [Math.round(f * 1000), 1000];
};

async function probe(url) {
  const j = JSON.parse(await execA(FFPROBE, ['-v', 'error', '-print_format', 'json', '-show_streams', '-show_format', url]));
  const v = j.streams.find((s) => s.codec_type === 'video'); if (!v) throw new Error('в файле нет видео');
  const a = j.streams.find((s) => s.codec_type === 'audio');
  const pick = (s) => { const [n, d] = String(s || '').split('/').map(Number); return n > 0 && d > 0 && n / d > 1 && n / d < 241 ? [n, d] : null; };
  const [fn, fd] = snapFps(...(pick(v.r_frame_rate) || pick(v.avg_frame_rate) || [30, 1]));
  let w = v.width, h = v.height;
  const rot = Number(v.tags?.rotate || v.side_data_list?.find((s) => s.rotation !== undefined)?.rotation || 0);
  if (Math.abs(rot) === 90) [w, h] = [h, w];
  return { w, h, fpsNum: fn, fpsDen: fd, fps: Math.round((fn / fd) * 1000) / 1000, dur: Number(j.format.duration || v.duration || 0), audio: !!a };
}

// ---------- uploader that streams finished segments while ffmpeg runs ----------
function uploader(outDir, keyPrefix, dirs, cancelled, transform) {
  const up = pool(6), uploaded = new Map(); // 20 раннеров × 6 — Blomp не захлёбывается
  const put = (rel) => {
    if (uploaded.has(rel)) return; uploaded.set(rel, -1);
    up.add(async () => {
      const f = path.join(outDir, rel); let buf = await fsp.readFile(f);
      if (transform && rel.endsWith('.m4s')) buf = await transform(rel, buf);
      await sput(`${keyPrefix}/${rel}`, buf, rel.endsWith('.m4s') ? 'video/iso.segment' : 'application/octet-stream');
      uploaded.set(rel, buf.length);
    });
  };
  const scan = async (final) => {
    if (cancelled()) return;
    for (const d of dirs) {
      const files = (await fsp.readdir(path.join(outDir, d)).catch(() => [])).filter((f) => f.endsWith('.m4s')).sort();
      for (const f of final ? files : files.slice(0, -1)) put(`${d}/${f}`);
    }
  };
  // проверка: каждый залитый файл реально есть в хранилище и нужного размера; пропавшие — перезаливаем
  const verify = async () => {
    let names = [...uploaded.keys()]; const all = names.slice();
    for (let round = 1; round <= 8; round++) {
      const bad = []; let i = 0;
      await Promise.all(Array.from({ length: 8 }, async () => { while (i < names.length) { const rel = names[i++];
        try { const sz = await shead(`${keyPrefix}/${rel}`); if (sz !== uploaded.get(rel)) bad.push(rel); } catch { bad.push(rel); } } }));
      if (!bad.length) { for (const rel of all) await fsp.unlink(path.join(outDir, rel)).catch(() => {}); return round; }
      log(`проверка: в хранилище нет ${bad.length} файлов, перезаливаю (попытка ${round})`);
      if (round === 8) throw new Error(`после 8 попыток в хранилище нет ${bad.length} файлов`);
      await sleep(Math.min(60000, 4000 * 2 ** (round - 1)));
      for (const rel of bad) uploaded.delete(rel);
      for (const rel of bad) put(rel);
      await up.drain(); if (up.err) throw up.err;
      names = bad; // дальше проверяем только перезалитые
    }
  };
  return { scan, put, up, uploaded, verify };
}

// ---------- tasks ----------
const killAll = () => { for (const k of kids) k.kill('SIGKILL'); };
let cancelled = false;
async function heartbeat(progress, info) {
  try { const r = await api('/w/hb', { progress, info }, { tries: 2 }); if (r.cancel && !cancelled) { cancelled = true; log('задача отменена диспетчером'); killAll(); } } catch (e) { if (e.fatal) { cancelled = true; killAll(); } }
}

async function taskA(spec, src) {
  const job = spec.job, out = path.join(WORK, 'a'); await fsp.mkdir(path.join(out, 'a'), { recursive: true }); await fsp.mkdir(path.join(out, 'sb'), { recursive: true });
  log('probe…');
  const info = await probe(src.url);
  log(`source ${info.w}x${info.h} @${info.fpsNum}/${info.fpsDen}, ${(info.dur / 3600).toFixed(2)} ч, audio=${info.audio}`);
  const plan = await api('/w/plan', { src: info });
  log(`план: ${plan.chunks} кусков по ~${plan.chunkSec} с`);
  const SEG = spec.settings.seg, SB_INT = spec.settings.sbInt;
  const vid = job.videoId;
  const result = { audio: null, sb: null };
  let pa = 0, ps = 0; const hb = setInterval(() => heartbeat(Math.min(0.99, info.audio ? pa * 0.7 + ps * 0.3 : ps), { stage: 'audio' }), 15000);
  const jobsP = [];
  if (info.audio) {
    const U = uploader(out, `v/${vid}`, ['a'], () => cancelled);
    const tick = setInterval(() => U.scan(false).catch(() => {}), 5000);
    jobsP.push(runFF(['-i', src.url, '-vn', '-map', '0:a:0', '-c:a', 'aac', '-b:a', spec.settings.audioKbps + 'k', '-ac', '2', '-ar', '48000',
      '-f', 'hls', '-hls_time', String(SEG), '-hls_playlist_type', 'vod', '-hls_segment_type', 'fmp4', '-hls_flags', 'independent_segments',
      '-hls_fmp4_init_filename', 'init.mp4', '-hls_segment_filename', path.join(out, 'a', 'seg_%05d.m4s'), path.join(out, 'a', 'index.m3u8')],
    (p) => { pa = Math.min(1, Number(p.out_time_us || 0) / 1e6 / info.dur); }).then(async () => {
      clearInterval(tick); await U.scan(true); await U.up.drain(); if (U.up.err) throw U.up.err; await U.verify();
      const init = await fsp.readFile(path.join(out, 'a', 'init.mp4')); await sput(`v/${vid}/a/init.mp4`, init, 'video/mp4');
      const segs = parsePlaylist(await fsp.readFile(path.join(out, 'a', 'index.m3u8'), 'utf8')).map(([f, d]) => [f, d, U.uploaded.get(`a/${f}`) || 0]);
      result.audio = { codec: 'mp4a.40.2', init: init.length, segs };
      log(`аудио готово: ${segs.length} сегментов`);
    }));
  }
  const webp = /libwebp/.test(execFileSync(FFMPEG, ['-hide_banner', '-encoders'], { encoding: 'utf8' }));
  const ext = webp ? 'webp' : 'jpg';
  jobsP.push(runFF(['-skip_frame', 'nokey', '-i', src.url, '-an', '-vf', `fps=1/${SB_INT},scale=160:90:force_original_aspect_ratio=decrease:flags=fast_bilinear,pad=160:90:-1:-1:color=black,tile=10x10`,
    '-fps_mode', 'passthrough', ...(webp ? ['-c:v', 'libwebp', '-quality', '55'] : ['-c:v', 'mjpeg', '-q:v', '6']), '-f', 'image2', path.join(out, 'sb', 'sb_%03d.' + ext)],
  (p) => { ps = Math.min(1, Number(p.out_time_us || 0) / 1e6 / info.dur); }).then(async () => {
    const sheets = (await fsp.readdir(path.join(out, 'sb'))).filter((f) => f.startsWith('sb_')).sort(); let bytes = 0;
    for (const f of sheets) { const b = await fsp.readFile(path.join(out, 'sb', f)); bytes += b.length; await sput(`v/${vid}/sb/${f}`, b, webp ? 'image/webp' : 'image/jpeg'); }
    if (sheets.length) result.sb = { i: SB_INT, w: 160, h: 90, c: 10, r: 10, sheets: sheets.length, ext, n: Math.ceil(info.dur / SB_INT), bytes };
  }).catch((e) => log('storyboard failed:', e.message)));
  if (!job.hasCover) {
    try {
      const pf = path.join(out, 'poster.' + ext);
      await execA(FFMPEG, ['-hide_banner', '-loglevel', 'error', '-y', '-ss', String(Math.floor(info.dur * 0.2)), '-i', src.url, '-frames:v', '1', '-vf', 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:-1:-1', ...(webp ? ['-c:v', 'libwebp', '-quality', '78'] : ['-q:v', '3']), pf]);
      await api('/w/cover', await fsp.readFile(pf), { raw: true });
    } catch (e) { log('poster failed:', e.message); }
  }
  await Promise.all(jobsP);
  clearInterval(hb);
  return result;
}

// аргументы ffmpeg: один вход → вся лесенка качеств в HLS fMP4 (общие для кусков стрима и вступления)
function ladderArgs({ L, info, st, out, pre, input, segName, initName }) {
  const SEG = st.seg, half = `${info.fpsNum}/${info.fpsDen * 2}`;
  let fg = `[0:v]${pre},format=yuv420p,split=${L.length}` + L.map((_, i) => `[b${i}]`).join('') + ';';
  fg += L.map((r, i) => `[b${i}]${r.half ? `fps=${half},` : ''}scale=-2:${r.h}:flags=bicubic[o${i}]`).join(';');
  const args = [...input, '-filter_complex', fg];
  L.forEach((r, i) => args.push('-map', `[o${i}]`));
  L.forEach((r, i) => {
    const fps = r.half ? info.fpsNum / info.fpsDen / 2 : info.fpsNum / info.fpsDen, g = Math.max(1, Math.round(fps * SEG));
    const kf = ['-force_key_frames:v:' + i, `expr:gte(t,n_forced*${SEG})`];
    if (r.codec === 'av1') args.push('-c:v:' + i, 'libsvtav1', '-preset:v:' + i, String(st.svtPreset), '-crf:v:' + i, String(r.crf), '-g:v:' + i, String(g), ...kf, '-svtav1-params:v:' + i, `tune=0:scd=0:enable-overlays=1:keyint=${g}`);
    else args.push('-c:v:' + i, 'libx264', '-preset:v:' + i, st.x264Preset, '-crf:v:' + i, String(r.crf), '-profile:v:' + i, 'high', '-g:v:' + i, String(g), '-keyint_min:v:' + i, String(g), '-sc_threshold:v:' + i, '0', ...kf);
    args.push('-maxrate:v:' + i, r.maxrate + 'k', '-bufsize:v:' + i, r.maxrate * 2 + 'k');
  });
  args.push('-an', '-f', 'hls', '-hls_time', String(SEG), '-hls_playlist_type', 'vod', '-hls_segment_type', 'fmp4', '-hls_flags', 'independent_segments',
    '-hls_fmp4_init_filename', initName, '-hls_segment_filename', path.join(out, '%v', segName),
    '-var_stream_map', L.map((r, i) => `v:${i},name:${r.name}`).join(' '), path.join(out, '%v', 'index.m3u8'));
  return args;
}

async function taskChunk(spec, src) {
  const k = Number(TASK), plan = spec.plan; if (!plan) throw new Error('нет плана');
  const ch = plan.chunkList[k]; if (!ch) throw new Error('нет куска ' + k);
  const { src: info, ladder: L } = plan, st = spec.settings, SEG = st.seg, vid = spec.job.videoId;
  const fr = `${info.fpsNum}/${info.fpsDen}`;
  const out = path.join(WORK, 'c' + k);
  for (const r of L) await fsp.mkdir(path.join(out, r.name), { recursive: true });
  const t0 = (ch.f0 * info.fpsDen) / info.fpsNum, dur = ch.frames ? (ch.frames * info.fpsDen) / info.fpsNum : info.dur - t0;
  log(`кусок ${k}/${plan.chunks}: кадры ${ch.f0}…${ch.frames ? ch.f0 + ch.frames : 'конец'} (${dur.toFixed(1)} с), лесенка ${L.map((r) => r.name).join(' ')}`);
  const seek = Math.max(0, t0 - 0.25 * info.fpsDen / info.fpsNum);
  const args = ladderArgs({ L, info, st, out, pre: `fps=${fr}${ch.frames ? `,trim=end_frame=${ch.frames}` : ''}`, segName: `c${String(k).padStart(3, '0')}_%05d.m4s`, initName: 'init.mp4',
    input: [...(seek > 0 ? ['-ss', seek.toFixed(6)] : []), ...(ch.frames ? ['-t', (dur + 3).toFixed(3)] : []), '-i', src.url] });
  // сдвиг времени: тики = f0 * den * timescale / num (точное целое для стандартных fps)
  const tsCache = {};
  const timescale = async (rname) => {
    if (tsCache[rname]) return tsCache[rname];
    for (let i = 0; i < 200; i++) { try { const f = (await fsp.readdir(path.join(out, rname))).find((x) => x.endsWith('.mp4')); const b = f ? await fsp.readFile(path.join(out, rname, f)) : Buffer.alloc(0); if (b.length > 100) return (tsCache[rname] = mdhdTimescale(b)); } catch {} await sleep(100); }
    throw new Error('нет init.mp4 для ' + rname);
  };
  const transform = async (rel, buf) => { const rname = rel.split('/')[0]; const ts = await timescale(rname); return shiftFrag(buf, Math.round((ch.f0 * info.fpsDen * ts) / info.fpsNum)); };
  const U = uploader(out, `v/${vid}`, L.map((r) => r.name), () => cancelled, transform);
  let frac = 0, speed = 0;
  const tick = setInterval(() => U.scan(false).catch(() => {}), 5000);
  const hb = setInterval(() => heartbeat(frac, { speed }), 15000);
  const tStart = Date.now();
  try {
    await runFF(args, (p) => { frac = Math.min(1, Number(p.out_time_us || 0) / 1e6 / dur); speed = parseFloat(p.speed) || 0; });
  } catch (e) { clearInterval(hb); throw e; } finally { clearInterval(tick); }
  if (cancelled) { clearInterval(hb); throw new Error('canceled'); }
  log(`закодировано за ${((Date.now() - tStart) / 60000).toFixed(1)} мин, дозаливка…`);
  await U.scan(true); await U.up.drain(); if (U.up.err) { clearInterval(hb); throw U.up.err; }
  await U.verify().finally(() => clearInterval(hb));
  const res = { renditions: {}, codecs: {}, inits: {} };
  for (const r of L) {
    const dir = path.join(out, r.name), files = await fsp.readdir(dir);
    const initF = files.find((f) => f.endsWith('.mp4')); if (!initF) throw new Error('нет init для ' + r.name);
    const init = await fsp.readFile(path.join(dir, initF));
    res.codecs[r.name] = codecString(init); res.inits[r.name] = init.length;
    if (k === 0) await sput(`v/${vid}/${r.name}/init.mp4`, init, 'video/mp4');
    res.renditions[r.name] = parsePlaylist(await fsp.readFile(path.join(dir, 'index.m3u8'), 'utf8')).map(([f, d]) => [f, d, U.uploaded.get(`${r.name}/${f}`) || 0]);
    if (!res.renditions[r.name].length) throw new Error('пустой выход ' + r.name);
  }
  return res;
}

(async () => {
  let src;
  try {
    const spec = await api('/w/spec');
    mask(spec.job.videoId); setCreds(spec.storage);
    await fsp.mkdir(WORK, { recursive: true }); for (const f of await fsp.readdir(WORK)) await fsp.rm(path.join(WORK, f), { recursive: true, force: true }); // саму папку не трогаем (/mnt/farm)
    if (spec.url) mask(spec.url);
    if (spec.sum?.llm) { mask(spec.sum.llm.url); mask(spec.sum.llm.key); try { mask(new URL(spec.sum.llm.url).host); } catch {} }
    if (TASK !== 'd' && TASK !== 'i') src = await sourceProxy(spec.job);
    await heartbeat(0, { stage: 'start' });
    const AC = require('./autocut')({ api, sget, sput, shead, runFF, execA, uploader, pool, heartbeat, log, mask, scrub, shiftFrag, mdhdTimescale, codecString, parsePlaylist, probe, ladderArgs, sleep, WORK, FFMPEG, FFPROBE, cancelled: () => cancelled });
    const result = TASK === 'a' ? await taskA(spec, src) : /^\d+$/.test(TASK) ? await taskChunk(spec, src) : await AC.run(TASK, spec, src);
    if (cancelled) { log('отменено'); process.exit(0); }
    await api('/w/done', { result });
    log('готово');
    process.exit(0);
  } catch (e) {
    log('ошибка:', e.message);
    if (!e.fatal && !cancelled) await api('/w/fail', { error: scrub(e.message).slice(0, 1500) }, { tries: 3 }).catch(() => {});
    process.exit(cancelled ? 0 : 1);
  } finally { src?.close(); }
})();
