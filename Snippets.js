const 小可爱文字解码器 = new TextDecoder('utf-8');
const 关门原因编码器 = new TextEncoder();
const 关门原因解码器 = new TextDecoder();
const 我的小甜甜身份证 = '88888888-8888-8888-8888-888888888888';  //建议修改成你自己的
const 身份证字节 = ((uuid) => {
  const hex = uuid.replace(/-/g, '');
  const arr = new Uint8Array(16);
  for (let i = 0; i < 16; i++) arr[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return arr;
})(我的小甜甜身份证);
const [SZ0, SZ1, SZ2, SZ3, SZ4, SZ5, SZ6, SZ7, SZ8, SZ9, SZ10, SZ11, SZ12, SZ13, SZ14, SZ15] = 身份证字节;
const 默认备用小可爱地址 = 'dsl253-007-079.nyc1.dsl.speakeasy.net';  //兜底落地IP，默认为某美国住宅IP，建议修改成你自己的


// ═══════════════════════════════════════════════════════════════════
// ⚙️  可调参数
// ═══════════════════════════════════════════════════════════════════
//
// ┌─ 合包最大字节 ────────────────────────────────────────────────────
// │  单次 WebSocket 发送帧的最大尺寸（字节）。
// │  积累的数据达到此上限时立即发送，不再等待定时器。
// │  • 调大：对千兆/超高速线路，减少帧数量，降低发送开销
// │  • 调小：对低带宽/高延迟线路，减少单帧延迟，改善首字节响应
// │  建议范围：64KB ~ 512KB
// │
// ├─ 合包刷新阈值 ────────────────────────────────────────────────────
// │  触发"提前发送"的积累字节数阈值，需 < 合包最大字节。
// │  积累量达到此值时不等定时器，立即刷出（类似 TCP Nagle 的 PSH）。
// │  • 通常设为 合包最大字节 × 75%
// │
// ├─ 合包最大等待 ────────────────────────────────────────────────────
// │  从第一个字节入队到强制刷出的最大等待时间（毫秒）。
// │  防止小包在低流量时被无限期积压。
// │  • 调小：降低小包延迟（对 HTTP/交互型流量友好）
// │  • 调大：让更多数据聚合发送（对大文件/流媒体友好）
// │  建议范围：4ms ~ 32ms
// │
// ├─ 桥梁缓冲水位 ────────────────────────────────────────────────────
// │  ws可读流（WS→TCP 方向）的背压高水位线（字节）。
// │  超过此水位时 pipeTo 会暂停读取，等 TCP 消费后再恢复。
// │  • 调大：对高带宽线路减少背压暂停次数，吞吐更平滑
// │  • 调小：减少内存占用（CF Workers 单个请求内存有限）
// │  建议范围：1MB ~ 16MB，不建议超过 32MB
// │
// ├─ 主连接超时毫秒 ──────────────────────────────────────────────────
// │  直连目标地址时的 TCP 握手超时（毫秒）。
// │  超时后自动降级到备用地址。
// │  • 国内节点：500 ~ 1500ms；跨国节点：1500 ~ 3000ms
// │
// ├─ 备用连接超时毫秒 ────────────────────────────────────────────────
// │  连接备用/兜底地址时的 TCP 握手超时（毫秒）。
// │  通常应比主连接超时稍长，给备用节点更多等待时间。
// │
// └─ 背压最大退避毫秒 ────────────────────────────────────────────────
//    WS→TCP 方向背压等待的最大退避时间（毫秒）。
//    当 TCP 写入缓冲区满时，消息队列会指数退避等待（1→2→4→…→此值）。
//    • 调大：TCP 慢速时减少无效轮询次数，降低 CPU 占用
//    • 调小：TCP 恢复后更快重新写入，减少额外延迟
//    建议范围：8ms ~ 64ms
//
// ────────────────────────────────────────────────────────────────────
// 【预设A：千兆高压】适合高带宽高并发场景（压测、速率峰值 > 500Mbps）
// const 合包最大字节        = 512 * 1024;   // 512KB 大帧，减少帧数
// const 合包刷新阈值        = 384 * 1024;   // 75% 阈值
// const 合包最大等待        = 16;           // 16ms，稍长聚合窗口
// const 桥梁缓冲水位        = 8 * 1024 * 1024; // 8MB 高水位
// const 主连接超时毫秒      = 1500;         // 激进超时，快速降级
// const 备用连接超时毫秒    = 3000;
// const 背压最大退避毫秒    = 32;

// 【预设B：千兆日常】适合日常使用（默认，兼顾延迟与吞吐）
const 合包最大字节       = 256 * 1024;      // 256KB 标准帧
const 合包刷新阈值       = 192 * 1024;      // 75% 阈值
const 合包最大等待       = 8;               // 8ms 快速刷出，对交互型流量友好
const 桥梁缓冲水位       = 4 * 1024 * 1024; // 4MB 水位，平衡内存与吞吐
const 主连接超时毫秒     = 2000;            // 2s 主连接超时
const 备用连接超时毫秒   = 4000;            // 4s 备用连接超时
const 背压最大退避毫秒   = 16;              // 16ms 最大退避

// ────────────────────────────────────────────────────────────────────
// 消息队列安全阀（一般无需修改）
// 单个连接最多积压的消息条数；超出时关闭连接防止内存耗尽
const 消息队列条数上限 = 512;
// 单个连接消息队列最大占用字节；超出时关闭连接（默认 64MB）
const 消息队列字节上限 = 64 * 1024 * 1024;

const 地址合法正则 = /^[a-zA-Z0-9._\-:\[\]]+$/;
const 路径IP正则 = /^\/ip=([^&\/]+)/;
const 握手确认包 = new Uint8Array([0, 0]);

function 截断关门原因(原因) {
  if (原因.length * 3 <= 123) return 原因;
  const encoded = 关门原因编码器.encode(原因);
  if (encoded.byteLength <= 123) return 原因;
  let len = 123;
  while (len > 0 && (encoded[len] & 0xc0) === 0x80) len--;
  return 关门原因解码器.decode(encoded.subarray(0, len));
}

function 校验候选地址(候选地址) {
  if (
    !候选地址 ||
    候选地址.length === 0 ||
    候选地址.length > 253 ||
    候选地址 === '.' ||
    候选地址 === '[]' ||
    候选地址 === '..' ||
    候选地址.startsWith('./') ||
    候选地址.startsWith('../') ||
    候选地址.startsWith(':') ||
    !地址合法正则.test(候选地址)
  ) {
    return 默认备用小可爱地址;
  }
  return 候选地址;
}

const txt缓存池 = new Map();
const txt请求池 = new Map();
const txt缓存生存期ms = 60 * 1000;
const txt失败冷却ms = 10 * 1000;

async function 获取动态地址(输入参数) {
  if (!输入参数 || (!输入参数.includes('://') && !输入参数.includes('/'))) {
    return 校验候选地址(输入参数 || 默认备用小可爱地址);
  }

  const 链接 = 输入参数.startsWith('http://') || 输入参数.startsWith('https://') ? 输入参数 : 'https://' + 输入参数;
  const 当前时间 = Date.now();
  const 缓存 = txt缓存池.get(链接);
  if (缓存 && 当前时间 < 缓存.过期时间) return 缓存.目标地址;

  if (txt请求池.has(链接)) return txt请求池.get(链接);

  const 请求Promise = (async () => {
    try {
      const 响应 = await fetch(链接, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        cf: { cacheTtl: 60 }
      });
      if (响应.ok) {
        let 文本 = await 响应.text();
        文本 = 文本.replace(/[\r\n\s\uFEFF]/g, '');
        const 校验结果 = 校验候选地址(文本);
        if (校验结果 !== 默认备用小可爱地址) {
          txt缓存池.set(链接, { 目标地址: 校验结果, 过期时间: Date.now() + txt缓存生存期ms });
          return 校验结果;
        }
        const 回退无效 = 缓存 ? 缓存.目标地址 : 默认备用小可爱地址;
        txt缓存池.set(链接, { 目标地址: 回退无效, 过期时间: Date.now() + txt失败冷却ms });
        return 回退无效;
      }
    } catch {}

    const 回退地址 = 缓存 ? 缓存.目标地址 : 默认备用小可爱地址;
    txt缓存池.set(链接, { 目标地址: 回退地址, 过期时间: Date.now() + txt失败冷却ms });
    return 回退地址;
  })().finally(() => txt请求池.delete(链接));

  txt请求池.set(链接, 请求Promise);
  return 请求Promise;
}

export default {
  async fetch(来自外面的请求) {
    const 握手头 = 来自外面的请求.headers.get('Upgrade');
    if (握手头 && 握手头.toLowerCase() === 'websocket') {
      const 网址 = new URL(来自外面的请求.url);
      const 携带SS通行证 = 网址.pathname.includes(我的小甜甜身份证);

      const 早期数据头 = 来自外面的请求.headers.get('sec-websocket-protocol');
      let 早期数据 = null;
      if (早期数据头) {
        try {
          早期数据 = 解码base64url(早期数据头);
        } catch {}
      }

      let 候选地址 = 默认备用小可爱地址;
      if (网址.searchParams.has('ip')) {
        候选地址 = 网址.searchParams.get('ip') || 默认备用小可爱地址;
      } else {
        const 提取路径IP = 网址.pathname.match(路径IP正则);
        if (提取路径IP) 候选地址 = decodeURIComponent(提取路径IP[1]);
      }

      const 当前备用地址 = await 获取动态地址(候选地址);
      return 升级成小可爱通道(来自外面的请求.fetcher, 当前备用地址, 携带SS通行证, 早期数据);
    }
    return new Response('OK', { status: 200 });
  },
};

// base64url 解码：模块级查表，仅初始化一次，所有连接共享
const _b64表 = (() => {
  const t = new Uint8Array(128);
  const _chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  for (let i = 0; i < 64; i++) t[_chars.charCodeAt(i)] = i;
  t['-'.charCodeAt(0)] = 62;
  t['_'.charCodeAt(0)] = 63;
  return t;
})();

function 解码base64url(str) {
  const 去等号 = str.replace(/=/g, '');
  const 长度 = 去等号.length;
  const 输出长度 = (长度 * 3 >> 2) - (str.endsWith('==') ? 2 : str.endsWith('=') ? 1 : 0);
  const 结果 = new Uint8Array(输出长度);
  let 写指针 = 0;
  const _lookup = (idx) => { const cc = 去等号.charCodeAt(idx); return cc < 128 ? (_b64表[cc] ?? 0) : 0; };

  for (let i = 0; i < 长度; i += 4) {
    const a = _lookup(i);
    const b = _lookup(i + 1);
    const c = i + 2 < 长度 ? _lookup(i + 2) : 0;
    const d = i + 3 < 长度 ? _lookup(i + 3) : 0;
    const n = (a << 18) | (b << 12) | (c << 6) | d;
    if (写指针 < 输出长度) 结果[写指针++] = n >> 16;
    if (写指针 < 输出长度) 结果[写指针++] = (n >> 8) & 0xff;
    if (写指针 < 输出长度) 结果[写指针++] = n & 0xff;
  }

  return 结果;
}


function 升级成小可爱通道(fetcher, 当前备用地址, 携带SS通行证, 早期数据) {
  const 泡泡对 = new WebSocketPair();
  const 小甜甜端 = 泡泡对[0];
  const 服务端 = 泡泡对[1];
  服务端.accept();
  服务端.binaryType = 'arraybuffer';
  开启数据小火车(fetcher, 服务端, 当前备用地址, 携带SS通行证, 早期数据).catch((e) => { console.error('[小火车]', e); });
  return new Response(null, {
    status: 101,
    webSocket: 小甜甜端,
    headers: { 'Sec-WebSocket-Extensions': '' }
  });
}

async function 开启数据小火车(fetcher, 服务端, 当前备用地址, 携带SS通行证, 早期数据) {
  let 小火车TCP通道;
  let 已经关门了 = false;
  let 流控制器;

  const ws可读流 = new ReadableStream({
    start(c) { 流控制器 = c; },
    cancel(reason) { 关门谢客(1011, reason?.message ?? 'stream cancelled'); },
  }, new ByteLengthQueuingStrategy({ highWaterMark: 桥梁缓冲水位 }));

  let 启动传输的信号;
  let 启动失败的信号;
  const 等待启动信号 = new Promise((resolve, reject) => {
    启动传输的信号 = resolve;
    启动失败的信号 = reject;
  });

  let 中止控制器 = null;

  function 关门谢客(代码 = 1011, 原因 = '再见啦', WS已先关闭 = false) {
    if (已经关门了) return;
    已经关门了 = true;

    if (!WS已先关闭) {
      try { 服务端.close(代码, 截断关门原因(原因)); } catch {}
    }

    if (代码 === 1000 && !启动失败的信号) {
      // 传输正常完成：pipeTo 已结束，流已关闭，启动信号已清空
      // 只需 abort + close TCP，不碰流控制器（流已关闭，error 会被吞掉）
    } else {
      // 建立前/异常关闭：用 error 中断 ws可读流的 pipeTo，用 reject 中断等待启动信号
      const 关门错误 = new Error(原因);
      try { 流控制器?.error(关门错误); } catch {}
      try { 启动失败的信号?.(关门错误); } catch {}
    }

    try { 中止控制器?.abort(); } catch {}
    try { 小火车TCP通道?.close?.(); } catch {}
  }

  async function 带超时的连接(主机, 端口, 超时ms) {
    if (已经关门了) throw new Error('已关门');
    let 炸弹定时器;
    const 通道 = fetcher.connect({ hostname: 主机, port: 端口 });
    通道.opened.catch(() => {});
    const 超时炸弹 = new Promise((_, reject) => {
      炸弹定时器 = setTimeout(() => reject(new Error('连接超时')), 超时ms);
    });
    try {
      await Promise.race([通道.opened, 超时炸弹]);
      return 通道;
    } catch (错误) {
      try { 通道.close(); } catch {}
      throw 错误;
    } finally {
      clearTimeout(炸弹定时器);
    }
  }

  服务端.addEventListener('close', () => 关门谢客(1000, '客户端挥手再见', true));
  服务端.addEventListener('error', () => 关门谢客(1011, 'WS出错啦', true));

  let 是第一个糖果包 = true;
  let 正在处理消息 = false;
  const 消息待办队列 = [];
  let 消息队列当前字节 = 0;
  let 消息队列读指针 = 0;

  const 触发消息处理 = () => {
    if (已经关门了 || 正在处理消息) return;
    正在处理消息 = true;
    (async () => {
      let 退避ms = 1;
      while (消息队列读指针 < 消息待办队列.length) {
        if (已经关门了) break;
        const 当前数据 = 消息待办队列[消息队列读指针++];
        消息队列当前字节 = Math.max(0, 消息队列当前字节 - 当前数据.byteLength);
        if (消息队列读指针 >= 64) {
          消息待办队列.splice(0, 消息队列读指针);
          消息队列读指针 = 0;
        }

        try {
          if (是第一个糖果包) {
            是第一个糖果包 = false;
            await 解读第一个糖果包(当前数据);
          } else {
            while (流控制器.desiredSize !== null && 流控制器.desiredSize <= 0) {
              if (已经关门了) break;
              await new Promise((r) => setTimeout(r, 退避ms));
              退避ms = Math.min(退避ms * 2, 背压最大退避毫秒);
            }
            退避ms = 1;
            if (已经关门了) break;
            try {
              流控制器.enqueue(当前数据);
            } catch {
              关门谢客(1011, '流已关闭');
              break;
            }
          }
        } catch {
          if (!已经关门了) 关门谢客(1011, '糖果包处理失败');
          break;
        }
      }
      消息待办队列.length = 0;
      消息队列读指针 = 0;
      消息队列当前字节 = 0;
      正在处理消息 = false;
    })().catch(() => {
      消息待办队列.length = 0;
      消息队列读指针 = 0;
      消息队列当前字节 = 0;
      正在处理消息 = false;
      if (!已经关门了) 关门谢客(1011, '消息队列崩溃');
    });
  };

  if (早期数据 && 早期数据.byteLength > 0) {
    消息待办队列.push(早期数据);
    消息队列当前字节 += 早期数据.byteLength;
    触发消息处理();
  }

  服务端.addEventListener('message', (事件) => {
    if (已经关门了) return;
    if (typeof 事件.data === 'string') {
      关门谢客(1008, '不支持文本帧');
      return;
    }

    const 数据 = 事件.data instanceof ArrayBuffer ? new Uint8Array(事件.data) : new Uint8Array(事件.data.buffer, 事件.data.byteOffset, 事件.data.byteLength);
    const 帧字节 = 数据.byteLength;
    if (
      消息待办队列.length >= 消息队列条数上限 ||
      消息队列当前字节 + 帧字节 > 消息队列字节上限
    ) {
      关门谢客(1011, '消息队列溢出');
      return;
    }

    消息待办队列.push(数据);
    消息队列当前字节 += 帧字节;
    触发消息处理();
  });

  async function 解读第一个糖果包(糖果数据) {
    const 缓冲区 = 糖果数据.buffer;
    const 视图偏移 = 糖果数据.byteOffset;
    const 有效长度 = 糖果数据.byteLength;
    const 视图 = new DataView(缓冲区, 视图偏移, 有效长度);

    if (有效长度 < 7) { 关门谢客(1008, '糖果包太短了'); return; }

    const 协议首字节 = 视图.getUint8(0);
    let 目标地址 = '';
    let 目标端口 = 0;
    let 数据负载起始 = 0;

    if (协议首字节 === 0) {
      if (有效长度 < 24) { 关门谢客(1008, '糖果包太短了'); return; }
      if (!身份证匹配(视图, 1)) { 关门谢客(1008, '身份证不对哦'); return; }

      const 附加长度 = 视图.getUint8(17);
      const cmd字节位 = 18 + 附加长度;
      if (cmd字节位 >= 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }

      const cmd = 视图.getUint8(cmd字节位);
      if (cmd !== 1) { 关门谢客(1008, '不支持的指令类型'); return; }

      const 端口起始位 = cmd字节位 + 1;
      if (端口起始位 + 2 > 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
      目标端口 = 视图.getUint16(端口起始位);
      if (目标端口 === 0) { 关门谢客(1008, '端口不合法'); return; }

      const 地址类型起始位 = 端口起始位 + 2;
      if (地址类型起始位 >= 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
      const 地址类型 = 视图.getUint8(地址类型起始位);
      let 地址字节长度 = 0;
      let 地址数据起始位 = 地址类型起始位 + 1;

      switch (地址类型) {
        case 1:
          if (地址数据起始位 + 4 > 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
          地址字节长度 = 4;
          目标地址 = `${视图.getUint8(地址数据起始位)}.${视图.getUint8(地址数据起始位 + 1)}.${视图.getUint8(地址数据起始位 + 2)}.${视图.getUint8(地址数据起始位 + 3)}`;
          break;
        case 2:
          if (地址数据起始位 >= 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
          地址字节长度 = 视图.getUint8(地址数据起始位);
          if (地址字节长度 === 0) { 关门谢客(1008, '地址为空'); return; }
          if (地址字节长度 > 253) { 关门谢客(1008, '域名过长'); return; }
          地址数据起始位 += 1;
          if (地址数据起始位 + 地址字节长度 > 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
          try {
            目标地址 = 小可爱文字解码器.decode(new Uint8Array(缓冲区, 视图偏移 + 地址数据起始位, 地址字节长度));
          } catch {
            关门谢客(1008, '域名解码失败');
            return;
          }
          break;
        case 3: {
          if (地址数据起始位 + 16 > 有效长度) { 关门谢客(1008, '糖果包太短了'); return; }
          地址字节长度 = 16;
          const b = 地址数据起始位;
          目标地址 =
            视图.getUint16(b).toString(16) + ':' +
            视图.getUint16(b + 2).toString(16) + ':' +
            视图.getUint16(b + 4).toString(16) + ':' +
            视图.getUint16(b + 6).toString(16) + ':' +
            视图.getUint16(b + 8).toString(16) + ':' +
            视图.getUint16(b + 10).toString(16) + ':' +
            视图.getUint16(b + 12).toString(16) + ':' +
            视图.getUint16(b + 14).toString(16);
          break;
        }
        default:
          关门谢客(1008, '不认识的地址类型');
          return;
      }

      数据负载起始 = 地址数据起始位 + 地址字节长度;
      try { 服务端.send(握手确认包); } catch {}
    } else if (协议首字节 === 1 || 协议首字节 === 3 || 协议首字节 === 4) {
      if (!携带SS通行证) { 关门谢客(1008, '抓到一只没有通行证的野猫'); return; }

      switch (协议首字节) {
        case 1:
          if (有效长度 < 7) { 关门谢客(1008, 'SS-V4包太短'); return; }
          目标地址 = `${视图.getUint8(1)}.${视图.getUint8(2)}.${视图.getUint8(3)}.${视图.getUint8(4)}`;
          目标端口 = 视图.getUint16(5);
          数据负载起始 = 7;
          break;
        case 3: {
          const 域名长度 = 视图.getUint8(1);
          if (域名长度 === 0) { 关门谢客(1008, 'SS-域名为空'); return; }
          if (有效长度 < 4 + 域名长度) { 关门谢客(1008, 'SS-域名包太短'); return; }
          try {
            目标地址 = 小可爱文字解码器.decode(new Uint8Array(缓冲区, 视图偏移 + 2, 域名长度));
          } catch {
            关门谢客(1008, 'SS-域名解码失败');
            return;
          }
          目标端口 = 视图.getUint16(2 + 域名长度);
          数据负载起始 = 4 + 域名长度;
          break;
        }
        case 4: {
          if (有效长度 < 19) { 关门谢客(1008, 'SS-V6包太短'); return; }
          目标地址 = [
            视图.getUint16(1).toString(16), 视图.getUint16(3).toString(16),
            视图.getUint16(5).toString(16), 视图.getUint16(7).toString(16),
            视图.getUint16(9).toString(16), 视图.getUint16(11).toString(16),
            视图.getUint16(13).toString(16), 视图.getUint16(15).toString(16),
          ].join(':');
          目标端口 = 视图.getUint16(17);
          数据负载起始 = 19;
          break;
        }
        default:
          关门谢客(1008, '不支持的神秘星际语言');
          return;
      }
    } else {
      关门谢客(1008, '不支持的神秘星际语言');
      return;
    }

    if (目标端口 === 0 || 数据负载起始 > 有效长度) { 关门谢客(1008, '地址段越界'); return; }

    const 首包剩余长度 = 有效长度 - 数据负载起始;

    try {
      const 临时通道 = await 带超时的连接(目标地址, 目标端口, 主连接超时毫秒);
      if (已经关门了) { try { 临时通道.close(); } catch {} return; }
      小火车TCP通道 = 临时通道;
    } catch {
      try {
        const { 备用主机, 备用端口 } = 拆分地址和端口(当前备用地址, 目标端口);
        if (!备用主机) { 关门谢客(1011, '备用地址主机为空'); return; }
        const 临时通道 = await 带超时的连接(备用主机, 备用端口, 备用连接超时毫秒);
        if (已经关门了) { try { 临时通道.close(); } catch {} return; }
        小火车TCP通道 = 临时通道;
      } catch {
        try {
          if (当前备用地址 === 默认备用小可爱地址) throw new Error('同址跳过');
          const { 备用主机, 备用端口 } = 拆分地址和端口(默认备用小可爱地址, 目标端口);
          if (!备用主机) { 关门谢客(1011, '兜底地址主机为空'); return; }
          const 临时通道 = await 带超时的连接(备用主机, 备用端口, 备用连接超时毫秒);
          if (已经关门了) { try { 临时通道.close(); } catch {} return; }
          小火车TCP通道 = 临时通道;
        } catch {
          关门谢客(1011, '所有路都堵死啦');
          return;
        }
      }
    }

    if (已经关门了) return;
    if (首包剩余长度 > 0) {
      try { 流控制器.enqueue(new Uint8Array(缓冲区, 视图偏移 + 数据负载起始, 首包剩余长度).slice()); } catch {}
    }

    启动传输的信号();
  }

  try {
    await 等待启动信号;
  } catch {
    return;
  }

  if (已经关门了) return;
  启动传输的信号 = null;
  启动失败的信号 = null;

  中止控制器 = new AbortController();
  const { signal: 中止信号 } = 中止控制器;

  await Promise.all([
    ws可读流.pipeTo(小火车TCP通道.writable, { signal: 中止信号 }).catch((e) => {
      if (e?.name !== 'AbortError') 关门谢客(1011, '桥梁→TCP中断');
    }),
    小火车TCP通道.readable.pipeTo(
      合包发送流(服务端, 合包最大字节, 合包刷新阈值, 合包最大等待),
      { signal: 中止信号 },
    ).catch((e) => {
      if (e?.name !== 'AbortError') 关门谢客(1011, 'TCP→WS异常');
    }),
  ]).catch(() => {});

  if (!已经关门了) 关门谢客(1000, '传输完成');
}

function 合包发送流(服务端, 最大字节, 刷新阈值, 最大等待ms) {
  const 积累缓冲 = [];
  let 积累字节数 = 0;
  let 定时器 = null;
  let 复用缓冲区 = new Uint8Array(最大字节);

  function 立刻发出去() {
    if (定时器) { clearTimeout(定时器); 定时器 = null; }
    if (积累缓冲.length === 0) return;
    if (服务端.readyState !== 1) {
      积累缓冲.length = 0;
      积累字节数 = 0;
      return;
    }

    let 合并包;
    if (积累缓冲.length === 1) {
      合并包 = 积累缓冲[0].slice();
    } else {
      if (积累字节数 > 复用缓冲区.byteLength) {
        复用缓冲区 = new Uint8Array(Math.max(最大字节, 积累字节数 * 2));
      } else if (积累字节数 < 复用缓冲区.byteLength >> 2 && 复用缓冲区.byteLength > 最大字节) {
        复用缓冲区 = new Uint8Array(Math.max(最大字节, 积累字节数));
      }
      let 写入位置 = 0;
      for (const 块 of 积累缓冲) {
        复用缓冲区.set(块, 写入位置);
        写入位置 += 块.byteLength;
      }
      合并包 = 复用缓冲区.subarray(0, 积累字节数).slice();
    }

    积累缓冲.length = 0;
    积累字节数 = 0;
    try { 服务端.send(合并包); } catch {}
  }

  return new WritableStream({
    write(chunk) {
      积累缓冲.push(chunk);
      积累字节数 += chunk.byteLength;
      if (积累字节数 >= 刷新阈值) {
        立刻发出去();
      } else if (!定时器) {
        定时器 = setTimeout(立刻发出去, 最大等待ms);
      }
    },
    flush() { 立刻发出去(); },
    abort() {
      if (定时器) { clearTimeout(定时器); 定时器 = null; }
      积累缓冲.length = 0;
      积累字节数 = 0;
    },
  }, new ByteLengthQueuingStrategy({ highWaterMark: 最大字节 }));
}

function 拆分地址和端口(地址字符串, 默认端口) {
  function 校验端口(端口) {
    return Number.isInteger(端口) && 端口 >= 1 && 端口 <= 65535 ? 端口 : 默认端口;
  }

  if (地址字符串.startsWith('[')) {
    const 括号结束 = 地址字符串.indexOf(']');
    if (括号结束 === -1) return { 备用主机: 地址字符串, 备用端口: 默认端口 };
    const 备用主机 = 地址字符串.slice(0, 括号结束 + 1);
    const 后缀 = 地址字符串.slice(括号结束 + 1);
    return {
      备用主机,
      备用端口: 校验端口(后缀.startsWith(':') ? Number(后缀.slice(1)) : 默认端口),
    };
  }

  const 冒号位 = 地址字符串.lastIndexOf(':');
  if (冒号位 === -1) return { 备用主机: 地址字符串, 备用端口: 默认端口 };
  if (地址字符串.indexOf(':') !== 冒号位) return { 备用主机: 地址字符串, 备用端口: 默认端口 };
  return {
    备用主机: 地址字符串.slice(0, 冒号位),
    备用端口: 校验端口(Number(地址字符串.slice(冒号位 + 1))),
  };
}

function 身份证匹配(视图, offset = 0) {
  return 视图.getUint8(offset) === SZ0 &&
    视图.getUint8(offset + 1) === SZ1 &&
    视图.getUint8(offset + 2) === SZ2 &&
    视图.getUint8(offset + 3) === SZ3 &&
    视图.getUint8(offset + 4) === SZ4 &&
    视图.getUint8(offset + 5) === SZ5 &&
    视图.getUint8(offset + 6) === SZ6 &&
    视图.getUint8(offset + 7) === SZ7 &&
    视图.getUint8(offset + 8) === SZ8 &&
    视图.getUint8(offset + 9) === SZ9 &&
    视图.getUint8(offset + 10) === SZ10 &&
    视图.getUint8(offset + 11) === SZ11 &&
    视图.getUint8(offset + 12) === SZ12 &&
    视图.getUint8(offset + 13) === SZ13 &&
    视图.getUint8(offset + 14) === SZ14 &&
    视图.getUint8(offset + 15) === SZ15;
}
