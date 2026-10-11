#!/usr/bin/env python3
"""V_N farm: распознавание речи + разделение на говорящих для одного куска стрима.
Вход: сырой PCM float32 16 кГц моно. Выход: JSON со словами (время, текст, говорящий, уверенность)
и «отпечатком голоса» каждого локального говорящего — по нему кусок s потом склеит людей по всему стриму.
  faster-whisper (large-v3, int8, CPU) — текст и пословные таймкоды
  sherpa-onnx (pyannote segmentation 3.0 + WeSpeaker ResNet34) — кто когда говорит, без токенов HF
"""
import argparse, json, os, sys, time, tarfile, urllib.request
import numpy as np

SR = 16000
CACHE = os.path.expanduser(os.environ.get('VN_ASR_CACHE', '~/.cache/vn-asr'))
SEG_URL = 'https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-segmentation-models/sherpa-onnx-pyannote-segmentation-3-0.tar.bz2'
EMB_URL = os.environ.get('VN_EMB_URL', 'https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-recongition-models/3dspeaker_speech_campplus_sv_zh_en_16k-common_advanced.onnx')

def say(*a): print(*a, flush=True)

def fetch(url, dst):
    if os.path.exists(dst) and os.path.getsize(dst) > 1000: return dst
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    for i in range(5):
        try:
            urllib.request.urlretrieve(url, dst + '.part'); os.replace(dst + '.part', dst); return dst
        except Exception as e:
            say('download retry', i, e); time.sleep(3 * (i + 1))
    raise RuntimeError('не скачалась модель ' + url)

def seg_model():
    d = os.path.join(CACHE, 'sherpa-onnx-pyannote-segmentation-3-0')
    m = os.path.join(d, 'model.onnx')
    if not os.path.exists(m):
        tb = fetch(SEG_URL, os.path.join(CACHE, 'seg.tar.bz2'))
        with tarfile.open(tb) as t: t.extractall(CACHE, filter='data')
    return m

def diarize(audio, threads):
    import sherpa_onnx as so
    emb_path = fetch(EMB_URL, os.path.join(CACHE, os.path.basename(EMB_URL)))
    cfg = so.OfflineSpeakerDiarizationConfig(
        segmentation=so.OfflineSpeakerSegmentationModelConfig(pyannote=so.OfflineSpeakerSegmentationPyannoteModelConfig(model=seg_model()), num_threads=threads),
        embedding=so.SpeakerEmbeddingExtractorConfig(model=emb_path, num_threads=threads),
        clustering=so.FastClusteringConfig(num_clusters=-1, threshold=float(os.environ.get('VN_DIAR_THRESHOLD', '0.55'))),
        min_duration_on=0.3, min_duration_off=0.5)
    if not cfg.validate(): raise RuntimeError('sherpa-onnx config invalid')
    sd = so.OfflineSpeakerDiarization(cfg)
    res = sd.process(audio).sort_by_start_time()
    segs = [(r.start, r.end, int(r.speaker)) for r in res]
    # отпечаток каждого говорящего: среднее эмбеддингов его самых длинных реплик (до ~90 с речи)
    ext = so.SpeakerEmbeddingExtractor(so.SpeakerEmbeddingExtractorConfig(model=emb_path, num_threads=threads))
    spk = {}
    for s, e, k in segs: spk.setdefault(k, []).append((s, e))
    out = {}
    for k, lst in spk.items():
        lst.sort(key=lambda x: x[0] - x[1]); total = sum(e - s for s, e in lst); acc = None; used = 0.0
        for s, e in lst:
            if e - s < 0.8 and acc is not None: break
            st = ext.create_stream(); st.accept_waveform(SR, audio[int(s * SR):int(e * SR)]); st.input_finished()
            if not ext.is_ready(st): continue
            v = np.array(ext.compute(st), dtype=np.float32); v /= (np.linalg.norm(v) + 1e-9)
            acc = v * (e - s) if acc is None else acc + v * (e - s); used += e - s
            if used > 90: break
        if acc is None: continue
        acc /= (np.linalg.norm(acc) + 1e-9)
        out[str(k)] = {'emb': [round(float(x), 5) for x in acc], 'dur': round(total, 2)}
    return segs, out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--audio', required=True); ap.add_argument('--out', required=True)
    ap.add_argument('--offset', type=float, default=0); ap.add_argument('--own', type=float, nargs=2, required=True)
    ap.add_argument('--model', default='large-v3'); ap.add_argument('--lang', default='ru'); ap.add_argument('--prompt', default='')
    ap.add_argument('--beam', type=int, default=5)
    a = ap.parse_args()
    threads = os.cpu_count() or 4
    audio = np.fromfile(a.audio, dtype=np.float32)
    dur = len(audio) / SR
    say(f'аудио {dur:.1f} с, модель {a.model}, потоков {threads}')
    from faster_whisper import WhisperModel
    t0 = time.time()
    model = WhisperModel(a.model, device='cpu', compute_type='int8', cpu_threads=threads, download_root=os.path.join(CACHE, 'whisper'))
    say(f'модель загружена за {time.time() - t0:.0f} с')
    lang = None if a.lang in ('', 'auto', 'none') else a.lang
    prompt = (a.prompt + '. ' if a.prompt else '') + 'Стрим, живая разговорная речь, с пунктуацией.'
    segments, info = model.transcribe(audio, language=lang, beam_size=a.beam, best_of=5, patience=1.0,
        temperature=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0], compression_ratio_threshold=2.4, log_prob_threshold=-1.0, no_speech_threshold=0.6,
        condition_on_previous_text=True, prompt_reset_on_temperature=0.5, initial_prompt=prompt,
        word_timestamps=True, hallucination_silence_threshold=2.0,
        vad_filter=True, vad_parameters=dict(min_silence_duration_ms=600, speech_pad_ms=250))
    words = []; last = 0
    for sg in segments:
        for w in (sg.words or []):
            words.append([w.start, w.end, w.word, -1, round(float(w.probability), 3)])
        if time.time() - last > 5: last = time.time(); say(f'PROGRESS {min(0.99, sg.end / max(1, dur)) * 0.8:.3f}')
    say(f'распознано {len(words)} слов за {time.time() - t0:.0f} с ({info.language})')
    t1 = time.time()
    try:
        dsegs, spk = diarize(audio, threads)
        say(f'говорящих в куске: {len(spk)}, реплик {len(dsegs)}, {time.time() - t1:.0f} с')
    except Exception as e:
        say('диаризация не удалась:', e); dsegs, spk = [], {}
    say('PROGRESS 0.97')
    # слово → говорящий с наибольшим перекрытием (или ближайший в пределах 1 с)
    j = 0
    for w in words:
        best, bo = -1, 0.0
        while j < len(dsegs) and dsegs[j][1] < w[0] - 5: j += 1
        for s, e, k in dsegs[j:j + 40]:
            if s > w[1] + 1: break
            o = min(e, w[1]) - max(s, w[0])
            if o > bo: bo, best = o, k
        if best < 0:
            near = [(min(abs(s - w[1]), abs(e - w[0])), k) for s, e, k in dsegs[j:j + 40] if abs(s - w[1]) < 1 or abs(e - w[0]) < 1]
            if near: best = min(near)[1]
        w[3] = best
    own0, own1 = a.own
    out = []
    for w in words:
        s = round(w[0] + a.offset, 2); e = round(w[1] + a.offset, 2)
        if own0 <= s < own1: out.append([s, e, w[2], w[3], w[4]])
    json.dump({'v': 1, 'lang': info.language, 'own': [own0, own1], 'words': out, 'speakers': spk,
               'turns': [[round(s + a.offset, 2), round(e + a.offset, 2), k] for s, e, k in dsegs]}, open(a.out, 'w'), ensure_ascii=False)
    say('PROGRESS 1')

if __name__ == '__main__':
    main()
