'use strict';
// V_N farm — задачи авто-нарезки (вызываются из runner.js):
//   d     — скачать стрим по ссылке (yt-dlp) и положить в хранилище частями, как обычную загрузку
//   t<k>  — кусок речи: faster-whisper large-v3 + разделение на говорящих (farm/asr.py)
//   s     — склейка речи по всему стриму → субтитры [1]/[2]… → нейросеть режет стрим на части (может просить отрывки видео)
//   i     — вступление: перекодировать под лесенку качеств этого стрима
//   p<k>  — собрать часть k из уже закодированных сегментов полного стрима (+ вступление, раскадровка, обложка, субтитры)
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const zlib = require('node:zlib');
const { spawn } = require('node:child_process');

module.exports = (X) => {
  const { api, sget, sput, shead, runFF, execA, pool, heartbeat, log, sleep, WORK, FFMPEG } = X;
  const T = (s) => { s = Math.max(0, Math.round(s)); const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60; return `${h}:${String(m).padStart(2, '0')}:${String(x).padStart(2, '0')}`; };
  const parseT = (v) => {
    if (typeof v === 'number') return v;
    const m = String(v || '').trim().replace(',', '.').match(/^(?:(\d+):)?(\d{1,3}):(\d{1,2}(?:\.\d+)?)$/);
    if (m) return (Number(m[1] || 0) * 3600) + Number(m[2]) * 60 + Number(m[3]);
    const n = parseFloat(v); return isFinite(n) ? n : NaN;
  };
  // залить список файлов и убедиться, что они реально лежат в хранилище (Blomp иногда «теряет» PUT)
  async function putVerified(items, conc = 6) {
    const allItems = items;
    const P = pool(conc);
    for (const it of items) P.add(async () => { await sput(it.key, it.buf ?? await fsp.readFile(it.file), it.type || 'application/octet-stream'); it.size = it.buf ? it.buf.length : (await fsp.stat(it.file)).size; });
    await P.drain(); if (P.err) throw P.err;
    for (let round = 1; ; round++) {
      const bad = []; const V = pool(16);
      for (const it of items) V.add(async () => { let sz = -1; try { sz = await shead(it.key); } catch {} if (sz !== it.size) bad.push(it); });
      await V.drain();
      if (!bad.length) return allItems;
      if (round >= 8) throw new Error(`в хранилище нет ${bad.length} файлов после 8 попыток`);
      log(`проверка: нет ${bad.length} файлов, перезаливаю`); await sleep(Math.min(60000, 4000 * 2 ** (round - 1)));
      items = bad;
      const R = pool(conc); for (const it of bad) R.add(async () => sput(it.key, it.buf ?? await fsp.readFile(it.file), it.type || 'application/octet-stream')); await R.drain(); if (R.err) throw R.err;
    }
  }
  function spawnLines(cmd, args, onLine, opts = {}) {
    return new Promise((resolve, reject) => {
      const p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
      let err = '', buf = '';
      const feed = (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i).replace(/\r/g, ''); buf = buf.slice(i + 1); try { onLine(l); } catch {} } };
      p.stdout.on('data', feed); p.stderr.on('data', (d) => { err = (err + d).slice(-4000); feed(String(d)); });
      p.on('error', reject);
      p.on('close', (c) => (c === 0 ? resolve() : reject(new Error(`${path.basename(cmd)} exit ${c}: ${X.scrub(err.slice(-1500))}`))));
    });
  }

  // ======================= d: скачать =======================
  async function taskD(spec) {
    const url = spec.url, dir = path.join(WORK, 'd'); await fsp.mkdir(dir, { recursive: true });
    const base = ['--no-playlist', '--no-warnings', '--js-runtimes', 'node', '--retries', '20', '--fragment-retries', '50', '--socket-timeout', '30'];
    if (process.env.YTDLP_COOKIES) { const cf = path.join(dir, 'cookies.txt'); await fsp.writeFile(cf, process.env.YTDLP_COOKIES); base.push('--cookies', cf); }
    if (process.env.YTDLP_PROXY) { X.mask(process.env.YTDLP_PROXY); base.push('--proxy', process.env.YTDLP_PROXY); }
    log('метаданные…');
    let meta = {};
    try { meta = JSON.parse(await execA('yt-dlp', [...base, '-J', '--skip-download', url])); } catch (e) { throw new Error('yt-dlp не смог открыть ссылку: ' + X.scrub(e.message).slice(-600)); }
    const maxH = Number(process.env.FARM_DL_MAXH || spec.settings.dlMaxH || 1080);
    const d8 = String(meta.upload_date || meta.release_date || '').replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');
    const info = { title: meta.title || meta.fulltitle || '', date: /^\d{4}-\d{2}-\d{2}$/.test(d8) ? d8 : '', uploader: meta.uploader || meta.channel || '', description: String(meta.description || '').slice(0, 4000), duration: meta.duration || 0, live: !!meta.is_live };
    log(`«${info.title}» · ${info.uploader} · ${info.duration ? (info.duration / 3600).toFixed(2) + ' ч' : 'длина ?'}${info.live ? ' · ИДЁТ ПРЯМО СЕЙЧАС — запишу до конца' : ''}`);
    const fmt = `bv*[height<=${maxH}]+ba/b[height<=${maxH}]/bv*+ba/b`;
    let got = 0, total = 0, last = 0;
    const hb = setInterval(() => heartbeat(Math.min(0.9, total ? (got / total) * 0.85 : 0.05), { stage: 'download' }), 15000);
    try {
      await spawnLines('yt-dlp', [...base, '-f', fmt, '-S', `res:${maxH},fps,vbr`, '--merge-output-format', 'mp4', '-N', '8', '--no-part', '--newline', ...(info.live ? ['--live-from-start'] : []),
        '--progress-template', 'download:VNP %(progress.downloaded_bytes)s %(progress.total_bytes)s %(progress.total_bytes_estimate)s', '-o', path.join(dir, 'src.%(ext)s'), url], (l) => {
        const m = /^VNP (\d+) (\S+) (\S+)/.exec(l); if (!m) return;
        got = Number(m[1]); total = Number(m[2]) || Number(m[3]) || total;
        if (Date.now() - last > 60000) { last = Date.now(); log(`скачано ${(got / 1048576).toFixed(0)} / ${(total / 1048576).toFixed(0)} МБ`); }
      });
    } finally { clearInterval(hb); }
    const file = (await fsp.readdir(dir)).filter((f) => f.startsWith('src.') && !/\.(part|ytdl|txt)$/.test(f)).map((f) => path.join(dir, f)).sort((a, b) => fs.statSync(b).size - fs.statSync(a).size)[0];
    if (!file) throw new Error('yt-dlp ничего не скачал');
    const size = (await fsp.stat(file)).size;
    log(`файл ${(size / 1073741824).toFixed(2)} GB, заливаю в хранилище…`);
    // как в загрузке из админки: части по ≥16 МБ, не больше 9000 частей
    const partSize = Math.max(16 * 1048576, Math.ceil(size / 9000 / 1048576) * 1048576), parts = Math.ceil(size / partSize);
    const fd = await fsp.open(file, 'r'); const P = pool(6); let up = 0;
    const hb2 = setInterval(() => heartbeat(0.9 + 0.09 * (up / parts), { stage: 'upload' }), 15000);
    const sizes = [];
    try {
      for (let i = 1; i <= parts; i++) P.add(async () => {
        const len = Math.min(partSize, size - (i - 1) * partSize), buf = Buffer.allocUnsafe(len);
        await fd.read(buf, 0, len, (i - 1) * partSize);
        const key = `raw/${spec.job.videoId}/p_${String(i).padStart(5, '0')}`;
        for (let a = 0; ; a++) { await sput(key, buf, 'application/octet-stream'); let sz = -1; try { sz = await shead(key); } catch {} if (sz === len) break; if (a >= 4) throw new Error('часть ' + i + ' не легла в хранилище'); await sleep(3000 * (a + 1)); }
        sizes[i] = len; up++;
      });
      await P.drain(); if (P.err) throw P.err;
    } finally { clearInterval(hb2); await fd.close(); }
    await fsp.rm(dir, { recursive: true, force: true });
    return { rawParts: parts, rawSize: size, partSize, meta: info };
  }

  // ======================= t<k>: речь =======================
  async function taskT(spec, src) {
    const A = spec.asr, a = Math.max(0, A.s - A.pad), b = Math.min(A.dur, A.e + A.pad);
    const dir = path.join(WORK, 't'); await fsp.mkdir(dir, { recursive: true });
    const pcm = path.join(dir, 'a.f32'), out = path.join(dir, 'a.json');
    log(`речь ${T(A.s)}–${T(A.e)} (с запасом ${T(a)}–${T(b)}), модель ${A.model}`);
    let frac = 0; const hb = setInterval(() => heartbeat(frac, { stage: 'asr' }), 15000);
    try {
      await runFF(['-ss', a.toFixed(3), '-t', (b - a).toFixed(3), '-i', src.url, '-vn', '-map', '0:a:0', '-ac', '1', '-ar', '16000', '-af', 'highpass=f=50', '-f', 'f32le', pcm], (p) => { frac = 0.08 * Math.min(1, Number(p.out_time_us || 0) / 1e6 / (b - a)); });
      const py = process.env.PYTHON || 'python3';
      await spawnLines(py, [path.join(__dirname, 'asr.py'), '--audio', pcm, '--out', out, '--offset', String(a), '--own', String(A.s), String(A.k === undefined || A.e >= A.dur ? A.e + 1 : A.e), '--model', A.model, '--lang', A.lang || 'auto', '--prompt', A.prompt || ''], (l) => {
        const m = /^PROGRESS ([\d.]+)/.exec(l); if (m) { frac = 0.08 + 0.9 * Number(m[1]); return; }
        if (l.trim() && !/^\s*$|Warning|warn\(/.test(l)) log('asr:', l.slice(0, 300));
      });
    } finally { clearInterval(hb); }
    const json = await fsp.readFile(out); const j = JSON.parse(json);
    const key = `farm/${spec.job.videoId}/asr/t${A.k}.json.gz`;
    await putVerified([{ key, buf: zlib.gzipSync(json, { level: 9 }), type: 'application/gzip' }]);
    await fsp.rm(dir, { recursive: true, force: true });
    return { key, tmp: [key], words: j.words.length, speakers: Object.keys(j.speakers || {}).length };
  }

  // ======================= s: субтитры + нейросеть =======================
  // склейка локальных говорящих всех кусков в людей по всему стриму: средняя связь по косинусу,
  // двух говорящих ОДНОГО куска не склеиваем (диаризация уже решила, что это разные люди)
  function clusterSpeakers(locals, sim) {
    const norm = (v) => { const n = Math.hypot(...v) || 1; return v.map((x) => x / n); };
    const dot = (a, b) => { let s = 0; for (let i = 0; i < a.length; i++) s += a[i] * b[i]; return s; };
    let C = locals.filter((l) => l.dur >= 1.5).map((l) => ({ sum: l.emb.map((x) => x * l.dur), dur: l.dur, ks: new Set([l.k]), m: [l] }));
    for (const c of C) c.c = norm(c.sum);
    for (;;) {
      let best = -1, bi = -1, bj = -1;
      for (let i = 0; i < C.length; i++) for (let j = i + 1; j < C.length; j++) {
        if ([...C[i].ks].some((k) => C[j].ks.has(k))) continue;
        const s = dot(C[i].c, C[j].c); if (s > best) { best = s; bi = i; bj = j; }
      }
      if (best < sim) break;
      const a = C[bi], b = C[bj];
      a.sum = a.sum.map((x, i) => x + b.sum[i]); a.dur += b.dur; b.ks.forEach((k) => a.ks.add(k)); a.m.push(...b.m); a.c = norm(a.sum);
      C.splice(bj, 1);
    }
    const map = {};
    C.forEach((c, g) => c.m.forEach((l) => { map[l.k + ':' + l.id] = g; }));
    for (const l of locals) if (!(l.k + ':' + l.id in map)) { // короткие реплики — к самому похожему, если похож
      let best = -1, g = -1; const e = norm(l.emb);
      C.forEach((c, i) => { const s = dot(c.c, e); if (s > best) { best = s; g = i; } });
      map[l.k + ':' + l.id] = best >= sim * 0.8 ? g : 1000 + Object.keys(map).length;
    }
    return map;
  }
  const HALLU = /(субтитр\S* (сделал|создавал|подготовил|делал|by)|редактор субтитров|корректор|dimatorzok|продолжение следует|спасибо за просмотр|подписывайтесь на (наш )?канал|ставьте лайк|amara\.org)/i;
  function buildSubs(chunks, sim) {
    const locals = [];
    chunks.forEach((c, k) => { for (const [id, sp] of Object.entries(c.speakers || {})) locals.push({ k, id, emb: sp.emb, dur: sp.dur }); });
    const map = clusterSpeakers(locals, sim);
    let W = [];
    chunks.forEach((c, k) => { for (const [s, e, t, spk, p] of c.words) W.push({ s, e: Math.max(e, s + 0.05), t, g: spk >= 0 ? map[k + ':' + spk] ?? -1 : -1, p: p ?? 1 }); });
    W.sort((a, b) => a.s - b.s);
    // слова без говорящего — от соседей; одиночные «вкрапления» другого говорящего (<0.7 с) сглаживаем
    for (let i = 0; i < W.length; i++) if (W[i].g < 0) W[i].g = W[i - 1]?.g ?? -1;
    for (let i = W.length - 1; i >= 0; i--) if (W[i].g < 0) W[i].g = W[i + 1]?.g ?? 0;
    for (let i = 1; i < W.length - 1; i++) { const a = W[i - 1].g, b = W[i + 1].g; if (W[i].g !== a && a === b && W[i].e - W[i].s < 0.7) W[i].g = a; }
    // нумерация по первому появлению, только заметные говорящие (>= 4 с речи)
    const talk = {}; for (const w of W) talk[w.g] = (talk[w.g] || 0) + (w.e - w.s);
    const num = {}; let n = 0;
    for (const w of W) if (!(w.g in num) && talk[w.g] >= 4) num[w.g] = ++n;
    for (const w of W) w.n = num[w.g] || 0;
    for (let i = 0; i < W.length; i++) if (!W[i].n) W[i].n = W[i - 1]?.n || W.find((x) => x.n)?.n || 1;
    // реплики-субтитры: смена говорящего, пауза, длина, знаки препинания
    const cues = []; let cur = null;
    const flush = () => { if (cur && cur.w.length) cues.push(cur); cur = null; };
    for (const w of W) {
      const txt = w.t.replace(/\s+/g, ' ');
      if (cur) {
        const len = cur.text.length + txt.length, dur = w.e - cur.s, gap = w.s - cur.e, lastCh = cur.text.trim().slice(-1);
        if (w.n !== cur.n || gap > 0.9 || len > 84 || dur > 7 || (/[.!?…]/.test(lastCh) && cur.text.length > 28) || (/[,;:]/.test(lastCh) && cur.text.length > 58)) flush();
      }
      if (!cur) cur = { s: w.s, e: w.e, n: w.n, text: '', w: [] };
      cur.text += txt; cur.e = w.e; cur.w.push(w);
    }
    flush();
    // мусор whisper: известные «галлюцинации» в тишине и зацикленные повторы
    const out = [];
    for (let i = 0; i < cues.length; i++) {
      const c = cues[i], t = c.text.trim(), avg = c.w.reduce((a, w) => a + w.p, 0) / c.w.length;
      if (HALLU.test(t) && (avg < 0.75 || c.w.length < 8)) continue;
      const key = t.toLowerCase().replace(/[^\p{L}\p{N} ]/gu, '');
      const prev = out.slice(-3).map((x) => x.key);
      if (key.length > 3 && prev.length === 3 && prev.every((k) => k === key)) continue;
      out.push({ ...c, t, key });
    }
    const speakers = n;
    const wrap = (t) => { if (t.length <= 42) return t; const mid = t.length / 2; let best = -1; for (let i = 0; i < t.length; i++) if (t[i] === ' ' && (best < 0 || Math.abs(i - mid) < Math.abs(best - mid))) best = i; return best > 0 ? t.slice(0, best) + '\n' + t.slice(best + 1) : t; };
    const res = out.map((c, i) => {
      const next = out[i + 1]; let e = Math.max(c.e + 0.25, c.s + 1); if (next && e > next.s - 0.04) e = Math.max(c.e, next.s - 0.04);
      const label = speakers > 1 ? `[${c.n}] ` : '';
      return [Math.round(c.s * 100) / 100, Math.round(e * 100) / 100, c.n, wrap(label + c.t)];
    });
    return { cues: res, speakers, lang: chunks[0]?.lang || 'ru' };
  }
  // компактная расшифровка для нейросети: реплики одного говорящего склеены в строки до ~30 с
  function transcript(subs) {
    const lines = []; let cur = null;
    for (const [s, e, n, text] of subs.cues) {
      const t = text.replace(/^\[\d+\] /, '').replace(/\n/g, ' ');
      if (cur && cur.n === n && s - cur.s < 30 && s - cur.e < 3) { cur.t += ' ' + t; cur.e = e; continue; }
      if (cur) lines.push(cur);
      cur = { s, e, n, t };
    }
    if (cur) lines.push(cur);
    return lines.map((l) => `[${T(l.s)}]${subs.speakers > 1 ? ` [${l.n}]` : ''} ${l.t}`).join('\n');
  }

  const SCHEMA = {
    type: 'object', additionalProperties: false,
    properties: {
      action: { type: 'string', enum: ['need_clips', 'final'], description: 'need_clips — нужно посмотреть отрывки видео; final — ответ готов' },
      notes: { type: 'string', description: 'кратко: что уже понятно о стриме и что увидено в отрывках' },
      clips: { type: 'array', description: 'какие отрывки видео показать (только при need_clips)', items: { type: 'object', additionalProperties: false,
        properties: { from: { type: 'string', description: 'Ч:ММ:СС' }, to: { type: 'string', description: 'Ч:ММ:СС' }, fps: { type: 'integer', description: 'кадров в секунду, 1–24' }, why: { type: 'string' } }, required: ['from', 'to', 'fps', 'why'] } },
      stream_title: { type: 'string', description: 'название полного стрима' },
      parts: { type: 'array', description: 'части стрима по порядку (только при final)', items: { type: 'object', additionalProperties: false,
        properties: { start: { type: 'string', description: 'Ч:ММ:СС' }, end: { type: 'string', description: 'Ч:ММ:СС' }, title: { type: 'string' }, description: { type: 'string' }, tags: { type: 'array', items: { type: 'string' } } },
        required: ['start', 'end', 'title', 'description', 'tags'] } },
    },
    required: ['action', 'notes', 'clips', 'stream_title', 'parts'],
  };
  function prompts(S, text, maxClips, clipMax) {
    const M = S.meta, who = M.streamer || M.uploader || 'стример';
    const sys = `Ты — опытный монтажёр, который делает нарезки со стримов ${who} для YouTube-канала с записями.
Тебе дают расшифровку стрима (таймкоды [Ч:ММ:СС]; [1], [2]… — разные говорящие, это номера, а не имена; номер ничего не говорит о том, кто это — стример обычно тот, кто говорит больше всех).
Твоя задача — разбить стрим на ЧАСТИ по смыслу: каждая часть — это одно занятие (смотрит конкретное видео/фильм/шоу, играет в конкретную игру, общается с чатом, готовит, едет куда-то, коллаб и т.д.) и станет отдельным видео.

Правила частей:
- Части идут по порядку и не пересекаются. Границы — там, где занятие реально сменилось (начал смотреть следующее видео, запустил другую игру). Начало части — чуть до начала занятия (когда он объявляет/включает), конец — когда закончил и перешёл к другому.
- Части короче ${Math.round(S.minPart / 60)} мин не делай: приклеивай их к соседней по смыслу (обычно к следующей) или выбрасывай, если это пустота (технические паузы, «отошёл», заставка). Болтовню с чатом между роликами приклеивай к соседнему ролику; отдельной частью — только если это длинное (от ~15 мин) самостоятельное общение.
- Не обязательно покрывать весь стрим: можно выбросить начало (ожидание, заставка) и конец (прощание), если там нет контента.
- Если стример много раз переключается между короткими роликами одной темы (например, подборки мемов, тиктоки), это одна часть.

Правила названий (ОЧЕНЬ ВАЖНО — строго в стиле примеров ниже):
- Формат: «${who} СМОТРИТ: <точное название ролика/фильма/шоу>», «${who} ИГРАЕТ в <ИГРА> / <с кем, если коллаб>», «${who} <ЧТО ДЕЛАЕТ> …» — как в примерах.
- Если смотрит ролик — используй ТОЧНОЕ оригинальное название ролика (как на YouTube, с оригинальным регистром и эмодзи, если они есть). Его видно на экране/вкладке браузера — если в расшифровке точного названия нет, ЗАПРОСИ ОТРЫВОК ВИДЕО, а не выдумывай.
- Игры — официальное название (латиницей, если оно латиницей), номер серии (#2) если это продолжение.
- Не используй номера говорящих в названиях. Не пиши в названиях даты и слово «нарезка».
- description: 1–3 предложения по-русски, о чём часть, без спойлеров-кликбейта. tags: 2–6 коротких тегов (игра/шоу/тип контента), в нижнем регистре.

Если по расшифровке нельзя уверенно понять, что именно он смотрит/во что играет (он не называет, говорит «вот это», «давай это глянем»), — верни action="need_clips" и попроси отрывки: до ${maxClips} отрывков за раз, каждый не длиннее ${Math.round(clipMax / 60)} мин; обычно хватает 20–90 секунд у начала части, где видно название. fps выбирай 1–24 (для чтения текста на экране хватает 1–2, для быстрого действия в игре — больше). Отрывки увидишь в следующем сообщении. Когда уверен — верни action="final" со ВСЕМИ частями.
Таймкоды в ответе — по расшифровке, формат Ч:ММ:СС. Отвечай строго JSON по схеме.`;
    const user = `Стример: ${who}
Исходное название записи: ${M.title || '—'}
Канал: ${M.uploader || '—'} · дата: ${M.date || '—'} · длительность: ${T(M.dur)}
${M.description ? 'Описание записи:\n' + M.description.slice(0, 2000) + '\n' : ''}
Примеры названий (стиль, которого надо держаться):
${(S.style || '').trim() || '—'}

Расшифровка стрима:
${text}`;
    return { sys, user };
  }
  // ---------- OpenAI-совместимый чат с ретраями (Codespace может «спать») ----------
  async function chat(L, messages, state) {
    const body = { model: L.model, messages, ...(L.effort ? { reasoning_effort: L.effort } : {}), ...(L.extra || {}) };
    if (state.fmt === 'schema') body.response_format = { type: 'json_schema', json_schema: { name: 'stream_cut', strict: true, schema: SCHEMA } };
    else if (state.fmt === 'json') body.response_format = { type: 'json_object' };
    for (let a = 0; ; a++) {
      let r, txt = '';
      try {
        r = await fetch(L.url + '/chat/completions', { method: 'POST', headers: { Authorization: 'Bearer ' + L.key, 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(20 * 60e3) });
        txt = await r.text();
      } catch (e) { if (a >= 9) throw new Error('нейросеть недоступна: ' + e.message); log(`нейросеть: ${e.message}, повтор через ${15 * (a + 1)} с`); await sleep(15000 * (a + 1)); continue; }
      if (r.ok) {
        let j; try { j = JSON.parse(txt); } catch { throw new Error('нейросеть вернула не JSON: ' + txt.slice(0, 300)); }
        const m = j.choices?.[0]?.message; let c = m?.content;
        if (Array.isArray(c)) c = c.map((x) => x.text || '').join('');
        if (!c && a < 4) { log('пустой ответ, повтор'); await sleep(5000); continue; }
        if (j.usage) log(`токены: вход ${j.usage.prompt_tokens}, выход ${j.usage.completion_tokens}`);
        return String(c || '');
      }
      const err = new Error(`нейросеть HTTP ${r.status}: ${txt.slice(0, 400)}`); err.status = r.status;
      if (r.status === 400 && state.fmt !== 'plain' && /response_format|json_schema|schema|structured/i.test(txt)) { state.fmt = state.fmt === 'schema' ? 'json' : 'plain'; log('структурированный ответ не поддерживается → ' + state.fmt); delete body.response_format; if (state.fmt === 'json') body.response_format = { type: 'json_object' }; continue; }
      if (r.status === 400 && body.reasoning_effort && /reasoning/i.test(txt)) { delete body.reasoning_effort; log('reasoning_effort не поддерживается — без него'); continue; }
      if ([408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524].includes(r.status) || r.status === 404 && a < 3) { if (a >= 9) throw err; log(`нейросеть HTTP ${r.status}, повтор через ${20 * (a + 1)} с`); await sleep(20000 * (a + 1)); continue; }
      throw err;
    }
  }
  const parseJSON = (s) => {
    s = s.replace(/^\s*```(?:json)?/i, '').replace(/```\s*$/, '');
    try { return JSON.parse(s); } catch {}
    const a = s.indexOf('{'), b = s.lastIndexOf('}'); if (a >= 0 && b > a) return JSON.parse(s.slice(a, b + 1));
    throw new Error('не JSON');
  };
  // ---------- отрывок видео для нейросети ----------
  async function makeClip(src, from, to, fps, L, budget, dir, idx) {
    const dur = to - from, H = L.res === 'medium' ? 480 : L.res === 'low' ? 360 : 720;
    if (L._mode === 'frames') {
      const n = Math.min(60, Math.max(4, Math.round(dur * Math.min(fps, 1)))), step = dur / n, imgs = [];
      for (let i = 0; i < n; i++) {
        const f = path.join(dir, `f${idx}_${i}.jpg`);
        await execA(FFMPEG, ['-hide_banner', '-loglevel', 'error', '-y', '-ss', (from + i * step).toFixed(2), '-i', src.url, '-frames:v', '1', '-vf', `scale=-2:${H}`, '-q:v', '4', f]).catch(() => {});
        if (fs.existsSync(f)) imgs.push({ t: from + i * step, b64: (await fsp.readFile(f)).toString('base64') });
      }
      return { kind: 'frames', imgs };
    }
    // fps как просили (≤ maxFps), но так, чтобы влезть в бюджет размера запроса и не превратить картинку в кашу
    const kbps = Math.min(H >= 720 ? 2500 : 1200, Math.max(60, Math.floor((budget * 8) / 1000 / dur) - 40));
    const fpsFit = Math.max(1, Math.min(L.maxFps || 24, fps || 2, Math.floor(kbps / (H >= 720 ? 45 : 25))));
    const f = path.join(dir, `c${idx}.mp4`);
    await runFF(['-ss', from.toFixed(2), '-t', dur.toFixed(2), '-i', src.url, '-map', '0:v:0', '-map', '0:a:0?', '-vf', `fps=${fpsFit},scale=-2:${H}:flags=bicubic`, '-c:v', 'libx264', '-preset', 'veryfast', '-b:v', kbps + 'k', '-maxrate', Math.round(kbps * 1.3) + 'k', '-bufsize', kbps * 2 + 'k',
      '-c:a', 'aac', '-b:a', '32k', '-ac', '1', '-movflags', '+faststart', f]);
    return { kind: 'video', b64: (await fsp.readFile(f)).toString('base64'), fps: fpsFit, kbps };
  }
  const MODES = ['image_url', 'file', 'video_url', 'frames'];
  function clipParts(c, mode) {
    if (c.kind === 'frames') return c.imgs.flatMap((im) => [{ type: 'text', text: `кадр ${T(im.t)}` }, { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + im.b64 } }]);
    const data = 'data:video/mp4;base64,' + c.b64;
    if (mode === 'file') return [{ type: 'file', file: { filename: 'clip.mp4', file_data: data } }];
    if (mode === 'video_url') return [{ type: 'video_url', video_url: { url: data } }];
    return [{ type: 'image_url', image_url: { url: data } }];
  }

  async function taskS(spec, src) {
    const S = spec.sum, L = { ...S.llm }, vid = spec.job.videoId, dur = S.meta.dur;
    let frac = 0, stage = 'subs'; const hb = setInterval(() => heartbeat(frac, { stage }), 15000);
    try {
      log(`склеиваю речь из ${S.asr.length} кусков…`);
      const chunks = [];
      for (const key of S.asr) chunks.push(JSON.parse(zlib.gunzipSync(await sget(key)).toString()));
      const subs = buildSubs(chunks, S.spkSim);
      log(`субтитров ${subs.cues.length}, говорящих ${subs.speakers}`);
      const subsJson = Buffer.from(JSON.stringify({ v: 1, lang: subs.lang, speakers: subs.speakers, cues: subs.cues }));
      await putVerified([{ key: `v/${vid}/subs.json`, buf: subsJson, type: 'application/json' }]);
      const subsInfo = { n: subs.cues.length, speakers: subs.speakers, file: 'subs.json' };
      frac = 0.1;
      const text = transcript(subs);
      log(`расшифровка для нейросети: ${text.length} символов`);
      if (!S.cut && !L.url) return { parts: [], chapters: null, subs: subsInfo, tmp: [] };
      stage = 'llm';
      const maxClips = Number(process.env.FARM_LLM_CLIPS || 4), budget = Number(process.env.FARM_LLM_MB || 18) * 1048576;
      const { sys, user } = prompts(S, text, maxClips, L.clipMax);
      const base = [{ role: 'system', content: sys }, { role: 'user', content: user }];
      let hist = [];
      const state = { fmt: 'schema' };
      let mode = L.videoMode === 'auto' || !MODES.includes(L.videoMode) ? 'image_url' : L.videoMode;
      const dir = path.join(WORK, 's'); await fsp.mkdir(dir, { recursive: true });
      const ask = async () => {
        for (let a = 0; ; a++) {
          const c = await chat(L, [...base, ...hist], state);
          try { return parseJSON(c); } catch {
            if (a >= 2) throw new Error('нейросеть не вернула JSON: ' + c.slice(0, 200));
            log('ответ не разобрался, прошу повторить');
            hist.push({ role: 'assistant', content: c.slice(0, 4000) }, { role: 'user', content: 'Ответ должен быть строго JSON по схеме (action, notes, clips, stream_title, parts). Повтори ответ.' });
          }
        }
      };
      let obj = await ask(), used = 0;
      log(`ответ: ${obj.action}${obj.clips?.length ? ', просит отрывков ' + obj.clips.length : ''}${obj.parts?.length ? ', частей ' + obj.parts.length : ''}`);
      while (obj.action === 'need_clips' && obj.clips?.length) {
        if (used >= L.rounds) {
          hist.push({ role: 'assistant', content: JSON.stringify(obj) }, { role: 'user', content: 'Лимит отрывков исчерпан. Верни action="final" со всеми частями по тому, что уже знаешь.' });
          obj = await ask(); break;
        }
        used++; frac = Math.min(0.9, 0.15 + used * 0.08);
        // старые отрывки из истории убираем (выводы модели остались в её notes) — чтобы не раздувать контекст
        hist = hist.map((m) => (Array.isArray(m.content) ? { role: m.role, content: m.content.filter((x) => x.type === 'text').map((x) => x.text).join('\n') + '\n(эти отрывки уже были показаны)' } : m));
        const want = obj.clips.slice(0, maxClips).map((c) => {
          let a = parseT(c.from), b = parseT(c.to); if (!(a >= 0)) a = 0; if (!(b > a)) b = a + 60;
          a = Math.min(a, Math.max(0, dur - 2)); b = Math.min(dur, b, a + L.clipMax);
          return { a, b, fps: Math.max(1, Math.min(24, Number(c.fps) || 2)), why: String(c.why || '').slice(0, 200) };
        });
        hist.push({ role: 'assistant', content: JSON.stringify(obj) });
        const msg = async () => {
          const parts = [{ type: 'text', text: `Вот запрошенные отрывки видео (осталось запросов: ${L.rounds - used}). Можешь запросить ещё или дать финальный ответ.` }];
          for (let i = 0; i < want.length; i++) {
            const w = want[i]; const c = await makeClip(src, w.a, w.b, w.fps, { ...L, _mode: mode === 'frames' ? 'frames' : 'video' }, budget / want.length, dir, `${used}_${i}`);
            parts.push({ type: 'text', text: `Отрывок ${i + 1}: ${T(w.a)}–${T(w.b)}${c.fps ? ` (${c.fps} к/с)` : ''}${w.why ? ' — ' + w.why : ''}` }, ...clipParts(c, mode));
          }
          return { role: 'user', content: parts };
        };
        for (;;) {
          hist.push(await msg());
          try { obj = await ask(); break; } catch (e) {
            // сервер не принял видео в таком виде → следующий способ (image_url → file → video_url → кадры)
            const next = MODES[MODES.indexOf(mode) + 1];
            if (!(e.status >= 400 && e.status < 500) || e.status === 429 || !next) throw e;
            log(`видео как «${mode}» не принято (${e.message.slice(0, 150)}) → пробую «${next}»`); mode = next; hist.pop();
          }
        }
        log(`после отрывков (${want.map((w) => T(w.a) + '–' + T(w.b)).join(', ')}): ${obj.action}${obj.notes ? ' · ' + obj.notes.slice(0, 200) : ''}`);
      }
      if (!Array.isArray(obj.parts)) obj.parts = [];
      await fsp.rm(dir, { recursive: true, force: true });
      // проверка и чистка частей
      let parts = (obj.parts || []).map((p) => ({ start: parseT(p.start), end: parseT(p.end), title: String(p.title || '').trim().slice(0, 200), description: String(p.description || '').trim().slice(0, 1500), tags: (Array.isArray(p.tags) ? p.tags : []).map((t) => String(t).toLowerCase().trim()).filter(Boolean).slice(0, 8) }))
        .filter((p) => p.title && p.start >= 0 && p.end > p.start).map((p) => ({ ...p, start: Math.max(0, Math.min(dur, p.start)), end: Math.min(dur, p.end) })).sort((a, b) => a.start - b.start);
      for (let i = 0; i < parts.length - 1; i++) if (parts[i].end > parts[i + 1].start) parts[i].end = parts[i + 1].start;
      parts = parts.filter((p) => p.end - p.start >= Math.min(60, S.minPart));
      for (const p of parts) p.description = (p.description ? p.description + '\n\n' : '') + `Фрагмент стрима${S.meta.date ? ' ' + S.meta.date : ''} · ${T(p.start)}–${T(p.end)}`;
      const chapters = parts.map((p) => ({ t: Math.round(p.start), title: p.title }));
      if (chapters.length && chapters[0].t > 60) chapters.unshift({ t: 0, title: 'Начало' });
      log(`итог: ${parts.length} частей`); for (const p of parts) log(`  ${T(p.start)}–${T(p.end)}  ${p.title}`);
      return { parts: S.cut ? parts : [], chapters, subs: subsInfo, streamTitle: String(obj.stream_title || '').slice(0, 200), tmp: [] };
    } finally { clearInterval(hb); }
  }

  // ======================= fMP4: время фрагментов =======================
  function boxes(buf, start, end, fn) {
    for (let o = start; o + 8 <= end;) {
      let size = buf.readUInt32BE(o); const type = buf.toString('latin1', o + 4, o + 8); let hdr = 8;
      if (size === 1) { size = Number(buf.readBigUInt64BE(o + 8)); hdr = 16; } else if (size === 0) size = end - o;
      if (size < hdr || o + size > end) break;
      fn(type, o + hdr, o + size); o += size;
    }
  }
  const trexDur = (init) => { let d = 0; const walk = (a, b) => boxes(init, a, b, (t, s, e) => { if (t === 'moov' || t === 'mvex') walk(s, e); else if (t === 'trex') d = init.readUInt32BE(s + 12); }); walk(0, init.length); return d; };
  // [начало, конец] фрагментов сегмента в тиках таймскейла дорожки
  function fragSpan(buf, defDur = 0) {
    let first = null, endMax = 0;
    boxes(buf, 0, buf.length, (t, s, e) => {
      if (t !== 'moof') return;
      boxes(buf, s, e, (t2, s2, e2) => {
        if (t2 !== 'traf') return;
        let base = 0, dd = defDur, sum = 0;
        boxes(buf, s2, e2, (t3, s3) => {
          if (t3 === 'tfhd') { const fl = buf.readUInt32BE(s3) & 0xffffff; let o = s3 + 8; if (fl & 1) o += 8; if (fl & 2) o += 4; if (fl & 8) dd = buf.readUInt32BE(o); }
          else if (t3 === 'tfdt') base = buf[s3] === 1 ? Number(buf.readBigUInt64BE(s3 + 4)) : buf.readUInt32BE(s3 + 4);
          else if (t3 === 'trun') {
            const fl = buf.readUInt32BE(s3) & 0xffffff, n = buf.readUInt32BE(s3 + 4); let o = s3 + 8; if (fl & 1) o += 4; if (fl & 4) o += 4;
            const per = ((fl & 0x100) ? 4 : 0) + ((fl & 0x200) ? 4 : 0) + ((fl & 0x400) ? 4 : 0) + ((fl & 0x800) ? 4 : 0);
            for (let i = 0; i < n; i++) { sum += (fl & 0x100) ? buf.readUInt32BE(o + i * per) : dd; }
          }
        });
        if (first === null) first = base; endMax = Math.max(endMax, base + sum);
      });
    });
    return [first ?? 0, endMax];
  }

  // ======================= i: вступление =======================
  async function taskI(spec) {
    const plan = spec.plan, info = plan.src, L = plan.ladder, st = spec.settings, vid = spec.job.videoId;
    const dir = path.join(WORK, 'i'); await fsp.mkdir(dir, { recursive: true });
    const inFile = path.join(dir, 'intro' + path.extname(spec.intro.key)); await fsp.writeFile(inFile, await sget(spec.intro.key));
    const pi = await X.probe(inFile).catch(() => null); if (!pi) throw new Error('вступление не читается ffmpeg');
    const fps = info.fpsNum / info.fpsDen, F = Math.max(1, Math.round(Math.min(pi.dur || 5, 60) * fps)), durV = (F * info.fpsDen) / info.fpsNum;
    log(`вступление ${pi.w}x${pi.h} ${pi.dur.toFixed(2)} с → ${info.w}x${info.h}@${(fps).toFixed(3)}, ${F} кадров, звук=${pi.audio}`);
    const out = path.join(dir, 'o'); for (const r of L) await fsp.mkdir(path.join(out, r.name), { recursive: true }); await fsp.mkdir(path.join(out, 'a'), { recursive: true });
    const W = info.w - (info.w % 2), H = info.h - (info.h % 2);
    await runFF(ladderArgsFor(L, info, st, out, inFile, `scale=${W}:${H}:force_original_aspect_ratio=decrease,pad=${W}:${H}:-1:-1:color=black,setsar=1,fps=${info.fpsNum}/${info.fpsDen},trim=end_frame=${F}`));
    const aIn = pi.audio ? ['-i', inFile] : ['-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo'];
    await runFF([...aIn, '-vn', '-map', '0:a:0', '-af', `aresample=48000,apad,atrim=0:${durV.toFixed(6)}`, '-c:a', 'aac', '-b:a', st.audioKbps + 'k', '-ac', '2', '-ar', '48000',
      '-f', 'hls', '-hls_time', String(st.seg), '-hls_playlist_type', 'vod', '-hls_segment_type', 'fmp4', '-hls_flags', 'independent_segments', '-hls_fmp4_init_filename', 'iinit.mp4',
      '-hls_segment_filename', path.join(out, 'a', 'i_%05d.m4s'), path.join(out, 'a', 'index.m3u8')]);
    const res = { dur: durV, renditions: {}, audio: null, tmp: [] }, items = [];
    for (const name of [...L.map((r) => r.name), 'a']) {
      const d = path.join(out, name), initF = path.join(d, (await fsp.readdir(d)).find((f) => f.endsWith('.mp4'))), init = await fsp.readFile(initF), ts = X.mdhdTimescale(init), dd = trexDur(init);
      const segs = X.parsePlaylist(await fsp.readFile(path.join(d, 'index.m3u8'), 'utf8'));
      let end = 0; const list = [];
      for (const [f, du] of segs) { const b = await fsp.readFile(path.join(d, f)); end = Math.max(end, fragSpan(b, dd)[1]); list.push([f, du, b.length]); items.push({ key: `farm/${vid}/intro/${name}/${f}`, file: path.join(d, f), type: 'video/iso.segment' }); }
      items.push({ key: `farm/${vid}/intro/${name}/iinit.mp4`, file: initF, type: 'video/mp4' });
      const R = { segs: list, ts, end, init: init.length, codec: name === 'a' ? 'mp4a.40.2' : X.codecString(init) };
      if (name === 'a') res.audio = R; else res.renditions[name] = R;
    }
    await putVerified(items);
    res.tmp = items.map((x) => x.key);
    await fsp.rm(dir, { recursive: true, force: true });
    return res;
  }
  const ladderArgsFor = (L, info, st, out, inFile, pre) => X.ladderArgs({ L, info, st, out, pre, input: ['-i', inFile], segName: 'i_%05d.m4s', initName: 'init.mp4' }); // ffmpeg с несколькими качествами принимает только init.mp4

  // ======================= p<k>: собрать часть из готовых сегментов =======================
  function playlistTxt(blocks) {
    const all = blocks.flatMap((b) => b.segs); const td = Math.ceil(Math.max(6, ...all.map((s) => s[1])));
    const L = ['#EXTM3U', '#EXT-X-VERSION:7', `#EXT-X-TARGETDURATION:${td}`, '#EXT-X-MEDIA-SEQUENCE:0', '#EXT-X-PLAYLIST-TYPE:VOD', '#EXT-X-INDEPENDENT-SEGMENTS'];
    blocks.forEach((b, i) => { if (i) L.push('#EXT-X-DISCONTINUITY'); L.push(`#EXT-X-MAP:URI="${b.init}"`); for (const [f, d] of b.segs) L.push(`#EXTINF:${d.toFixed(6)},`, f); });
    L.push('#EXT-X-ENDLIST', ''); return L.join('\n');
  }
  const rinfo = (segs, dur) => { let bytes = 0, peak = 0; for (const [, d, sz] of segs) { bytes += sz; peak = Math.max(peak, (sz * 8) / Math.max(0.5, d || 6)); } return { segs: segs.length, bytes, bw: Math.round(peak), abw: Math.round((bytes * 8) / Math.max(1, dur)) }; };
  async function taskP(spec, src) {
    const { part, full, intro, plan } = spec, vid = spec.job.videoId, pid = part.id, info = plan.src, SEG = spec.settings.seg;
    X.mask(pid);
    const dir = path.join(WORK, 'p'); await fsp.mkdir(path.join(dir, 'sb'), { recursive: true });
    const starts = (segs) => { const o = []; let t = 0; for (const s of segs) { o.push(t); t += s[1]; } o.push(t); return o; };
    const nearest = (arr, t) => { let bi = 0; for (let i = 0; i < arr.length; i++) if (Math.abs(arr[i] - t) < Math.abs(arr[bi] - t)) bi = i; return bi; };
    const L = plan.ladder, ref = full.renditions[L[0].name], st0 = starts(ref);
    let i0 = nearest(st0.slice(0, -1), part.start), i1 = nearest(st0, part.end); if (i1 <= i0) i1 = Math.min(ref.length, i0 + 1);
    const introDur = intro ? intro.dur : 0;
    log(`часть ${part.i}: ${T(part.start)}–${T(part.end)} → сегменты ${i0}…${i1 - 1} (${T(st0[i0])}–${T(st0[i1])}), вступление ${introDur ? introDur.toFixed(2) + ' с' : 'нет'}`);
    let done = 0, todo = 0, frac = 0; const hb = setInterval(() => heartbeat(Math.min(0.95, todo ? (done / todo) * 0.8 + frac * 0.15 : 0), { stage: 'part' }), 15000);
    const media = { v: 1, segDur: SEG, renditions: [], audio: null, sb: null, profile: spec.job.opts?.profile || 'av1', part: { from: st0[i0], to: st0[i1], parent: vid } };
    let total = 0, shiftSec = 0, T0v = 0;
    try {
      // видео: сдвиг времени так, чтобы часть начиналась сразу после вступления (общий сдвиг для всех качеств — по эталонному)
      const P = pool(12);
      const copy = async (from, to, transform, type) => { const b = await sget(from); const o = transform ? transform(b) : b; await sput(to, o, type); const sz = await shead(to).catch(() => -1); if (sz !== o.length) { await sleep(2000); await sput(to, o, type); } done++; return o.length; };
      for (const [ri, r] of L.entries()) {
        const segs = full.renditions[r.name], mine = segs.slice(i0, i1);
        const init = await sget(`v/${vid}/${r.name}/init.mp4`), ts = X.mdhdTimescale(init);
        await copy(`v/${vid}/${r.name}/init.mp4`, `v/${pid}/${r.name}/init.mp4`, null, 'video/mp4');
        const first = await sget(`v/${vid}/${r.name}/${mine[0][0]}`), tf0 = fragSpan(first)[0];
        if (ri === 0) { T0v = tf0 / ts; shiftSec = introDur - T0v; }
        const ir = intro?.renditions?.[r.name];
        const introTicks = ir ? Math.round((ir.end / ir.ts) * ts) : 0, delta = introTicks - tf0;
        todo += mine.length + (ir ? ir.segs.length + 1 : 0);
        if (ir) { P.add(() => copy(`farm/${vid}/intro/${r.name}/iinit.mp4`, `v/${pid}/${r.name}/iinit.mp4`, null, 'video/mp4')); for (const [f] of ir.segs) P.add(() => copy(`farm/${vid}/intro/${r.name}/${f}`, `v/${pid}/${r.name}/${f}`, null, 'video/iso.segment')); }
        for (const [f] of mine) P.add(() => copy(`v/${vid}/${r.name}/${f}`, `v/${pid}/${r.name}/${f}`, (b) => X.shiftFrag(b, delta), 'video/iso.segment'));
        const blocks = [...(ir ? [{ init: 'iinit.mp4', segs: ir.segs }] : []), { init: 'init.mp4', segs: mine }];
        await sput(`v/${pid}/${r.name}/index.m3u8`, Buffer.from(playlistTxt(blocks)), 'application/vnd.apple.mpegurl');
        const all = blocks.flatMap((b) => b.segs), dur = all.reduce((a, s) => a + s[1], 0), ri2 = rinfo(all, dur);
        total += ri2.bytes + init.length + (ir?.init || 0);
        media.renditions.push({ name: r.name, codec: full.codecs[r.name], ...ri2, h: r.h, w: Math.round((info.w * r.h) / info.h / 2) * 2, fps: r.fps, kind: r.codec });
      }
      // звук: тот же сдвиг в секундах, что у видео → синхрон сохраняется
      if (full.audio) {
        const segs = full.audio.segs, sa = starts(segs); let a0 = nearest(sa.slice(0, -1), T0v), a1 = nearest(sa, st0[i1]); if (a1 <= a0) a1 = Math.min(segs.length, a0 + 1);
        const init = await sget(`v/${vid}/a/init.mp4`), ts = X.mdhdTimescale(init);
        let delta = Math.round(shiftSec * ts);
        const firstA = fragSpan(await sget(`v/${vid}/a/${segs[a0][0]}`))[0];
        if (firstA + delta < 0) { a0++; }
        const mine = segs.slice(a0, a1), ia = intro?.audio;
        todo += mine.length + 1 + (ia ? ia.segs.length + 1 : 0);
        await copy(`v/${vid}/a/init.mp4`, `v/${pid}/a/init.mp4`, null, 'video/mp4');
        if (ia) { P.add(() => copy(`farm/${vid}/intro/a/iinit.mp4`, `v/${pid}/a/iinit.mp4`, null, 'video/mp4')); for (const [f] of ia.segs) P.add(() => copy(`farm/${vid}/intro/a/${f}`, `v/${pid}/a/${f}`, null, 'video/iso.segment')); }
        for (const [f] of mine) P.add(() => copy(`v/${vid}/a/${f}`, `v/${pid}/a/${f}`, (b) => X.shiftFrag(b, delta), 'video/iso.segment'));
        const blocks = [...(ia ? [{ init: 'iinit.mp4', segs: ia.segs }] : []), { init: 'init.mp4', segs: mine }];
        await sput(`v/${pid}/a/index.m3u8`, Buffer.from(playlistTxt(blocks)), 'application/vnd.apple.mpegurl');
        const all = blocks.flatMap((b) => b.segs), ri2 = rinfo(all, all.reduce((a, s) => a + s[1], 0));
        total += ri2.bytes + init.length + (ia?.init || 0);
        media.audio = { name: 'a', codec: full.audio.codec, ...ri2 };
      }
      // раскадровка + обложка + субтитры — пока копируются сегменты
      const partDur = st0[i1] - st0[i0], D = introDur + partDur;
      const webp = /libwebp/.test(await execA(FFMPEG, ['-hide_banner', '-encoders'])); const ext = webp ? 'webp' : 'jpg', SB = spec.settings.sbInt;
      const sbP = runFF(['-skip_frame', 'nokey', '-ss', Math.max(0, T0v - introDur).toFixed(2), '-t', D.toFixed(2), '-i', src.url, '-an', '-vf', `fps=1/${SB},scale=160:90:force_original_aspect_ratio=decrease:flags=fast_bilinear,pad=160:90:-1:-1:color=black,tile=10x10`,
        '-fps_mode', 'passthrough', ...(webp ? ['-c:v', 'libwebp', '-quality', '55'] : ['-c:v', 'mjpeg', '-q:v', '6']), '-f', 'image2', path.join(dir, 'sb', 'sb_%03d.' + ext)], (p) => { frac = Math.min(1, Number(p.out_time_us || 0) / 1e6 / D); })
        .then(async () => { const sheets = (await fsp.readdir(path.join(dir, 'sb'))).filter((f) => f.startsWith('sb_')).sort(); let bytes = 0; const it = []; for (const f of sheets) { const fp = path.join(dir, 'sb', f); bytes += (await fsp.stat(fp)).size; it.push({ key: `v/${pid}/sb/${f}`, file: fp, type: webp ? 'image/webp' : 'image/jpeg' }); } await putVerified(it, 6); if (sheets.length) media.sb = { i: SB, w: 160, h: 90, c: 10, r: 10, sheets: sheets.length, ext, n: Math.ceil(D / SB) }; total += bytes; })
        .catch((e) => log('раскадровка не удалась:', e.message));
      try {
        const pf = path.join(dir, 'poster.' + ext);
        await execA(FFMPEG, ['-hide_banner', '-loglevel', 'error', '-y', '-ss', String(Math.floor(T0v + partDur * 0.3)), '-i', src.url, '-frames:v', '1', '-vf', 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:-1:-1', ...(webp ? ['-c:v', 'libwebp', '-quality', '78'] : ['-q:v', '3']), pf]);
        await api('/w/cover', await fsp.readFile(pf), { raw: true });
      } catch (e) { log('обложка не удалась:', e.message); }
      if (spec.subs) {
        try {
          const j = JSON.parse((await sget(`v/${vid}/subs.json`)).toString()), t1 = st0[i1];
          const cues = j.cues.filter(([a, b]) => b > T0v && a < t1).map(([a, b, n, t]) => [Math.max(introDur, Math.round((a + shiftSec) * 100) / 100), Math.round((Math.min(b, t1) + shiftSec) * 100) / 100, n, t]);
          await putVerified([{ key: `v/${pid}/subs.json`, buf: Buffer.from(JSON.stringify({ ...j, cues })), type: 'application/json' }]);
          media.subs = { n: cues.length, speakers: j.speakers, file: 'subs.json' };
        } catch (e) { log('субтитры части не удались:', e.message); }
      }
      await P.drain(); if (P.err) throw P.err;
      await sbP;
      media.renditions.sort((a, b) => b.h - a.h || (a.kind === 'av1' ? -1 : 1));
      const ab = media.audio ? media.audio.bw : 0;
      let m3u = '#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n';
      if (media.audio) m3u += '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Main",DEFAULT=YES,AUTOSELECT=YES,CHANNELS="2",URI="a/index.m3u8"\n';
      for (const r of media.renditions) m3u += `#EXT-X-STREAM-INF:BANDWIDTH=${r.bw + ab},AVERAGE-BANDWIDTH=${r.abw + (media.audio?.abw || 0)},CODECS="${r.codec}${media.audio ? ',' + media.audio.codec : ''}",RESOLUTION=${r.w}x${r.h},FRAME-RATE=${r.fps.toFixed(3)}${media.audio ? ',AUDIO="aud"' : ''}\n${r.name}/index.m3u8\n`;
      await putVerified([{ key: `v/${pid}/master.m3u8`, buf: Buffer.from(m3u), type: 'application/vnd.apple.mpegurl' }]);
      media.totalBytes = total;
      await fsp.rm(dir, { recursive: true, force: true });
      log(`часть собрана: ${T(D)}, ${(total / 1048576).toFixed(0)} МБ, файлов ${done}`);
      return { media, duration: D, size: total, tmp: [] };
    } finally { clearInterval(hb); }
  }

  return {
    run: (task, spec, src) => (task === 'd' ? taskD(spec) : task === 's' ? taskS(spec, src) : task === 'i' ? taskI(spec) : /^t\d+$/.test(task) ? taskT(spec, src) : /^p\d+$/.test(task) ? taskP(spec, src) : Promise.reject(new Error('неизвестная задача ' + task))),
    _test: { buildSubs, transcript, clusterSpeakers, parseT, fragSpan, playlistTxt, SCHEMA, prompts },
  };
};
