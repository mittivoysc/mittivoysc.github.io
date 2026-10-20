/* MittivoyScreen Cloud — подключение к доске по коду, без IP-адреса.
 * Через бесплатные публичные MQTT-серверы передаются только служебные сообщения для установки соединения.
 * Само видео идёт напрямую между ноутбуком и доской по сети офиса (WebRTC).
 * Используется и на сайте (отправитель), и на доске (приёмник).
 */
(function () {
  'use strict';

  const DEFAULT_BROKERS = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt',
  ];
  const PREFIX = 'mardonscreen/v1/';
  const ICE = [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun.cloudflare.com:3478' }];

  // ---------------- мини-клиент MQTT 3.1.1 поверх WebSocket ----------------
  const enc = new TextEncoder(), dec = new TextDecoder();

  function varint(n) { const out = []; do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 128; out.push(b); } while (n > 0); return out; }
  function str(s) { const b = enc.encode(s); return [b.length >> 8, b.length & 255, ...b]; }
  function packet(type, body) { return new Uint8Array([type, ...varint(body.length), ...body]); }

  class Mqtt {
    constructor(url) {
      this.url = url; this.handlers = []; this.subs = new Set(); this.closed = false;
      this.buf = new Uint8Array(0); this.pid = 1; this.onclose = null;
    }

    connect(timeoutMs = 7000) {
      return new Promise((resolve, reject) => {
        let done = false;
        const fail = e => { if (!done) { done = true; try { this.ws.close(); } catch {} reject(e); } };
        const timer = setTimeout(() => fail(new Error('timeout')), timeoutMs);
        try { this.ws = new WebSocket(this.url, ['mqtt']); } catch (e) { clearTimeout(timer); return fail(e); }
        this.ws.binaryType = 'arraybuffer';
        this.ws.onopen = () => {
          const id = 'mardon-' + Math.random().toString(36).slice(2, 12);
          // CONNECT: протокол MQTT 4, чистая сессия, keepalive 30 с
          const body = [...str('MQTT'), 4, 2, 0, 30, ...str(id)];
          this.ws.send(packet(0x10, body));
        };
        this.ws.onmessage = e => {
          const chunk = new Uint8Array(e.data);
          const merged = new Uint8Array(this.buf.length + chunk.length);
          merged.set(this.buf); merged.set(chunk, this.buf.length); this.buf = merged;
          this._drain(() => { if (!done) { done = true; clearTimeout(timer); this._startPing(); resolve(this); } });
        };
        this.ws.onerror = () => fail(new Error('ws error'));
        this.ws.onclose = () => {
          clearInterval(this.pingT);
          if (!done) return fail(new Error('closed'));
          if (!this.closed && this.onclose) this.onclose();
        };
      });
    }

    _startPing() { this.pingT = setInterval(() => { try { this.ws.send(new Uint8Array([0xC0, 0])); } catch {} }, 20000); }

    _drain(onConnack) {
      while (this.buf.length >= 2) {
        let mult = 1, len = 0, i = 1, b;
        do { if (i >= this.buf.length) return; b = this.buf[i++]; len += (b & 127) * mult; mult *= 128; } while (b & 128);
        if (this.buf.length < i + len) return;
        const type = this.buf[0], body = this.buf.slice(i, i + len);
        this.buf = this.buf.slice(i + len);
        const kind = type >> 4;
        if (kind === 2) { if (body[1] === 0) onConnack(); }                 // CONNACK
        else if (kind === 3) {                                               // PUBLISH
          const tl = (body[0] << 8) | body[1];
          const topic = dec.decode(body.slice(2, 2 + tl));
          let p = 2 + tl;
          if ((type >> 1) & 3) p += 2;                                       // id пакета для QoS>0
          const payload = dec.decode(body.slice(p));
          for (const h of this.handlers) { try { h(topic, payload); } catch (e) { console.error(e); } }
        }
      }
    }

    subscribe(topic) {
      if (this.subs.has(topic)) return;
      this.subs.add(topic);
      const id = this.pid++ & 0xffff || 1;
      this.ws.send(packet(0x82, [id >> 8, id & 255, ...str(topic), 0]));
    }

    unsubscribe(topic) {
      if (!this.subs.delete(topic)) return;
      const id = this.pid++ & 0xffff || 1;
      try { this.ws.send(packet(0xA2, [id >> 8, id & 255, ...str(topic)])); } catch {}
    }

    publish(topic, obj) {
      const payload = enc.encode(typeof obj === 'string' ? obj : JSON.stringify(obj));
      this.ws.send(packet(0x30, [...str(topic), ...payload]));
    }

    on(h) { this.handlers.push(h); }
    close() { this.closed = true; clearInterval(this.pingT); try { this.ws.send(new Uint8Array([0xE0, 0])); this.ws.close(); } catch {} }
  }

  // Первый сервер, который ответит (подключаемся ко всем сразу, лишние закрываем)
  function connectAny(urls) {
    return new Promise((resolve, reject) => {
      let left = urls.length, won = false;
      for (const u of urls) {
        const m = new Mqtt(u);
        m.connect().then(c => { if (won) c.close(); else { won = true; resolve(c); } })
          .catch(() => { if (--left === 0 && !won) reject(new Error('no broker')); });
      }
    });
  }

  function waitIce(pc, ms) {
    if (pc.iceGatheringState === 'complete') return Promise.resolve();
    return new Promise(res => {
      const t = setTimeout(res, ms);
      pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') { clearTimeout(t); res(); } });
    });
  }

  // Браузеры прячут локальный адрес за «xxxx.local» — подставляем настоящий IP доски
  function fixSdp(sdp, ip) {
    return ip ? sdp.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.local/gi, ip) : sdp;
  }

  // ---------------- ДОСКА: принимает показы по коду ----------------
  // opts: { brokers, onOffer(id, name, sdp) -> Promise<answerSdp|null>, onBye(id), onStatus(bool) }
  function board(opts) {
    const brokers = opts.brokers || DEFAULT_BROKERS;
    const clients = new Map();   // url -> Mqtt
    let code = null;
    const topic = () => PREFIX + code + '/board';

    function handle(client) {
      return async (t, raw) => {
        if (t !== topic()) return;
        let m; try { m = JSON.parse(raw); } catch { return; }
        if (!m || typeof m.from !== 'string' || !/^[a-z0-9]{6,20}$/.test(m.from)) return;
        const reply = obj => { try { client.publish(PREFIX + code + '/s/' + m.from, obj); } catch {} };
        if (m.type === 'offer' && typeof m.sdp === 'string') {
          const res = await opts.onOffer(m.from, String(m.name || 'Гость').slice(0, 40), m.sdp, reply);
          if (res && res.sdp) reply({ type: 'answer', sdp: res.sdp });
          else reply({ type: res && res.error ? res.error : 'busy' });
        } else if (m.type === 'bye') opts.onBye(m.from);
      };
    }

    function status() { if (opts.onStatus) opts.onStatus(clients.size > 0); }

    function open(url, delay = 0) {
      setTimeout(() => {
        const c = new Mqtt(url);
        c.connect().then(() => {
          clients.set(url, c);
          c.on(handle(c));
          if (code) c.subscribe(topic());
          c.onclose = () => { clients.delete(url); status(); open(url, 5000); };
          status();
        }).catch(() => open(url, Math.min(60000, (delay || 3000) * 2)));
      }, delay);
    }
    brokers.forEach(u => open(u));

    return {
      setCode(c) {
        if (c === code) return;
        const old = code ? topic() : null;
        code = c;
        for (const cl of clients.values()) { if (old) cl.unsubscribe(old); cl.subscribe(topic()); }
      },
      online: () => clients.size > 0,
    };
  }

  // ---------------- НОУТБУК: показывает экран по коду ----------------
  // Возвращает { pc, stop() }. События: onState('connecting'|'live'|'failed'|'kicked'|'busy'|'notfound'|'closed')
  async function share({ code, name, stream, brokers, onState, maxBitrate = 8000000, client }) {
    const mq = client || await connectAny(brokers || DEFAULT_BROKERS);
    const id = Math.random().toString(36).slice(2, 12);
    const myTopic = PREFIX + code + '/s/' + id, boardTopic = PREFIX + code + '/board';
    let ended = false;
    const end = state => { if (ended) return; ended = true; try { pc.close(); } catch {} onState && onState(state); };

    const pc = new RTCPeerConnection({ iceServers: ICE });
    stream.getTracks().forEach(t => pc.addTrack(t, stream));
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'connected') onState && onState('live');
      if (pc.connectionState === 'failed') end('failed');
    };

    mq.subscribe(myTopic);
    let answered = false;
    mq.on(async (t, raw) => {
      if (t !== myTopic || ended) return;
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.type === 'answer' && !answered) {
        answered = true;
        await pc.setRemoteDescription({ type: 'answer', sdp: m.sdp });
        for (const s of pc.getSenders()) {
          if (s.track && s.track.kind === 'video') {
            const p = s.getParameters();
            if (!p.encodings || !p.encodings.length) p.encodings = [{}];
            p.encodings[0].maxBitrate = maxBitrate; p.encodings[0].maxFramerate = 30;
            try { await s.setParameters(p); } catch {}
          }
        }
      } else if (m.type === 'busy') end('busy');
      else if (m.type === 'kick') end('kicked');
    });

    onState && onState('connecting');
    await pc.setLocalDescription(await pc.createOffer());
    await waitIce(pc, 2500);
    mq.publish(boardTopic, { type: 'offer', from: id, name, sdp: pc.localDescription.sdp });
    setTimeout(() => { if (!answered) end('notfound'); }, 9000);

    return {
      pc,
      stop() {
        if (!ended) { try { mq.publish(boardTopic, { type: 'bye', from: id }); } catch {} }
        end('closed');
        mq.unsubscribe(myTopic);
      },
    };
  }

  window.MardonCloud = { Mqtt, connectAny, board, share, fixSdp, waitIce, ICE, DEFAULT_BROKERS };
})();
