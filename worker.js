/*
 * ==========================================================
 * Coupang AI Discord Bot Server
 * Cloudflare Worker + Durable Object
 * ==========================================================
 *
 * [수정 내용] 봇에 "대기 없이" 반복 접속하던 문제 해결
 *
 *  1. 재접속 대기 시간 통합
 *     - 타이머 / Cron / /status 어디서 호출하든 같은
 *       "다음 접속 가능 시각(nextConnectAt)"을 지킨다.
 *     - 실패가 이어질수록 대기 시간이 늘어난다.
 *       (5초 → 10초 → 20초 → ... 최대 5분)
 *
 *  2. 오래된 소켓의 이벤트 무시
 *     - 이미 교체된 소켓의 close 이벤트가 새 소켓 정보를 지우고
 *       중복 접속을 연쇄적으로 만들던 문제를 막는다.
 *       (소켓은 항상 최대 1개, 모든 핸들러가 현재 소켓인지 확인)
 *
 *  3. 재시도해도 소용없는 종료 코드(4004, 4010~4014)는
 *     10분에 한 번만 다시 시도하고, 원인을 lastError 에 표시한다.
 *
 *  4. Identify 보호
 *     - 5.5초 간격 + 24시간 500회 제한
 *       (Discord 한도: 1000회/24시간, 초과하면 봇 토큰이 재설정됨)
 *     - 끊겨도 가능하면 Identify 대신 Resume 으로 복구한다.
 *
 *  5. Heartbeat ACK 확인으로 끊긴 연결을 감지한다.
 *     (기존의 9분 강제 재연결은 제거)
 *
 *  6. 같은 메시지를 두 번 처리하지 않고, Discord 429 응답이 오면
 *     retry_after 만큼 기다렸다가 다시 보낸다.
 */

const DISCORD_GATEWAY =
  "wss://gateway.discord.gg/?v=10&encoding=json";

const DISCORD_API =
  "https://discord.com/api/v10";

const OPENROUTER_MODEL =
  "openrouter/free";

/* GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT */
const INTENTS =
  (1 << 0) |
  (1 << 9) |
  (1 << 15);


/*
 * ==========================================================
 * 재접속 / 대기 정책
 * ==========================================================
 */

/* 안정적으로 쓰던 연결이 끊겼을 때 첫 재접속 대기 */
const RECONNECT_FIRST_MS = 2 * 1000;

/* 연속으로 실패할 때의 시작 대기 (5초 → 10초 → 20초 ...) */
const RECONNECT_BASE_MS = 5 * 1000;

/* 재접속 대기 최대치 */
const RECONNECT_MAX_MS = 5 * 60 * 1000;

/* 이 시간 이상 READY 상태였으면 "안정적"으로 보고 실패 횟수를 0으로 */
const STABLE_CONNECTION_MS = 60 * 1000;

/* 접속 후 READY 까지 기다리는 최대 시간 */
const CONNECT_TIMEOUT_MS = 60 * 1000;

/* 토큰 오류 / Intent 오류 등 복구 불가능한 종료 후 재시도 간격 */
const FATAL_COOLDOWN_MS = 10 * 60 * 1000;

/* Discord 가 속도 제한(4008)으로 끊었을 때 최소 대기 */
const RATE_LIMIT_COOLDOWN_MS = 60 * 1000;

/* Identify 보호 */
const IDENTIFY_MIN_GAP_MS = 5500;
const IDENTIFY_DAILY_LIMIT = 500;
const DAY_MS = 24 * 60 * 60 * 1000;

/* 처리한 메시지 ID 를 기억하는 개수 */
const SEEN_MESSAGE_LIMIT = 500;

/* 다시 접속해도 소용없는 종료 코드 */
const FATAL_CLOSE_CODES = new Set(
  [4004, 4010, 4011, 4012, 4013, 4014]
);

/* 세션이 무효가 되는 종료 코드 (Resume 불가 → 새로 Identify) */
const RESET_SESSION_CODES = new Set(
  [1000, 1001, 4007, 4009]
);

const CLOSE_CODE_INFO = {
  4000: "알 수 없는 오류",
  4001: "알 수 없는 opcode",
  4002: "payload 디코딩 오류",
  4003: "인증 전에 payload 전송",
  4004: "인증 실패 - 봇 토큰 확인 필요",
  4005: "이미 인증됨",
  4007: "잘못된 sequence",
  4008: "요청 속도 제한",
  4009: "세션 시간 초과",
  4010: "잘못된 shard",
  4011: "shard 필요",
  4012: "잘못된 API 버전",
  4013: "잘못된 Intent",
  4014: "허용되지 않은 Intent - Developer Portal 에서 MESSAGE CONTENT INTENT 활성화 필요"
};


function errMsg(error) {
  return error && error.message
    ? String(error.message)
    : String(error);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}


export default {

  /*
   * ==========================================================
   * HTTP 요청
   * ==========================================================
   */
  async fetch(request, env) {

    const url = new URL(request.url);


    /*
     * ----------------------------------------------------------
     * /status
     * ----------------------------------------------------------
     *
     * 연결 확인 + 최신 상태 반환.
     * 대기 시간(백오프) 중이면 새로 접속하지 않고 상태만 돌려준다.
     */
    if (url.pathname === "/status") {

      try {

        const id =
          env.DISCORD_BOT.idFromName("main");

        const stub =
          env.DISCORD_BOT.get(id);

        return await stub.fetch(
          "https://discord-bot/connect?by=status"
        );

      } catch (error) {

        return new Response(
          JSON.stringify(
            {
              worker: true,
              error: errMsg(error)
            },
            null,
            2
          ),
          {
            status: 500,
            headers: {
              "Content-Type":
                "application/json; charset=utf-8"
            }
          }
        );

      }

    }


    /*
     * 기본 페이지
     */
    return new Response(
      "Coupang AI Discord Bot Server",
      {
        status: 200
      }
    );

  },


  /*
   * ==========================================================
   * Cloudflare Cron (매분)
   * ==========================================================
   *
   * 연결이 끊겨 있으면 다시 접속을 시도한다.
   * (대기 시간 중이면 아무것도 하지 않는다.)
   */
  async scheduled(event, env, ctx) {

    try {

      const id =
        env.DISCORD_BOT.idFromName("main");

      const stub =
        env.DISCORD_BOT.get(id);

      ctx.waitUntil(
        stub.fetch(
          "https://discord-bot/connect?by=cron"
        )
      );

    } catch (error) {

      console.error(
        "Cron 실행 실패:",
        error
      );

    }

  }

};


/*
 * ==========================================================
 * Discord Durable Object
 * ==========================================================
 */

export class DiscordBot {

  constructor(state, env) {

    this.state = state;
    this.env = env;


    /* 현재 "유효한" Discord WebSocket (항상 최대 1개) */
    this.socket = null;
    this.socketCreatedAt = 0;


    /* 연결 단계 */
    this.connectMode = "identify";   // "identify" | "resume"
    this.helloHandled = false;
    this.ready = false;
    this.readyAt = 0;


    /* 세션 정보 (끊겼을 때 Resume 에 사용) */
    this.sessionId = null;
    this.resumeUrl = null;
    this.sequence = null;
    this.botUserId = null;


    /* Heartbeat */
    this.heartbeatStartTimer = null;
    this.heartbeatTimer = null;
    this.heartbeatAcked = true;


    /* 재접속 대기 */
    this.reconnectTimer = null;
    this.nextConnectAt = 0;
    this.failCount = 0;


    /* Identify 기록 (24시간 한도 보호, storage 에도 저장) */
    this.identifyLog = [];


    /* 이미 처리한 메시지 ID */
    this.seenMessageIds = new Set();


    /* 상태 정보 */
    this.status = {

      createdAt: new Date().toISOString(),

      lastCron: null,
      lastConnectCheck: null,

      lastAction: "Durable Object created",

      lastEvent: null,
      lastEventTime: null,

      lastHeartbeatAt: null,
      lastHeartbeatAckAt: null,

      lastError: null,

      gateway: "not_connected"

    };


    /* 저장된 Identify 기록 불러오기 */
    this.state.blockConcurrencyWhile(async () => {

      try {

        const saved =
          await this.state.storage.get("identifyLog");

        if (Array.isArray(saved)) {

          const now = Date.now();

          this.identifyLog = saved.filter(
            t =>
              typeof t === "number" &&
              now - t < DAY_MS
          );

        }

      } catch (error) {

        this.status.lastError =
          "identifyLog 불러오기 실패: " +
          errMsg(error);

      }

    });

  }


  /*
   * ==========================================================
   * Durable Object 요청 처리
   * ==========================================================
   */
  async fetch(request) {

    const url = new URL(request.url);


    /*
     * 연결 확인 + 상태 반환 (by = cron | status ...)
     */
    if (url.pathname === "/connect") {

      const by =
        url.searchParams.get("by") || "unknown";

      if (by === "cron") {

        this.status.lastCron =
          new Date().toISOString();

      }

      this.connect(by);

      return this.json(this.snapshot());

    }


    /*
     * 상태만 확인 (접속 시도 없음)
     */
    if (url.pathname === "/status") {

      return this.json(this.snapshot());

    }


    return new Response(
      "Not Found",
      {
        status: 404
      }
    );

  }


  json(data) {

    return new Response(
      JSON.stringify(data, null, 2),
      {
        headers: {
          "Content-Type":
            "application/json; charset=utf-8",
          "Cache-Control":
            "no-store"
        }
      }
    );

  }


  snapshot() {

    const now = Date.now();

    return {

      ...this.status,

      connected:
        !!this.socket &&
        this.socket.readyState === WebSocket.OPEN,

      identified:
        this.ready,

      socketState:
        this.getSocketState(),

      connectMode:
        this.connectMode,

      canResume:
        this.canResume(),

      failCount:
        this.failCount,

      nextConnectInSeconds:
        Math.max(
          0,
          Math.ceil((this.nextConnectAt - now) / 1000)
        ),

      identifyCountLast24h:
        this.countIdentifies(now)

    };

  }


  /*
   * ==========================================================
   * WebSocket 상태
   * ==========================================================
   */
  getSocketState() {

    if (!this.socket) {
      return "null";
    }

    if (this.socket.readyState === WebSocket.CONNECTING) {
      return "CONNECTING";
    }

    if (this.socket.readyState === WebSocket.OPEN) {
      return "OPEN";
    }

    if (this.socket.readyState === WebSocket.CLOSING) {
      return "CLOSING";
    }

    if (this.socket.readyState === WebSocket.CLOSED) {
      return "CLOSED";
    }

    return "UNKNOWN";

  }


  countIdentifies(now) {

    return this.identifyLog.filter(
      t => now - t < DAY_MS
    ).length;

  }


  /*
   * ==========================================================
   * Discord Gateway 연결
   * ==========================================================
   *
   * 타이머 / Cron / /status 모두 이 함수만 호출한다.
   * 몇 번을 호출해도 안전하다.
   *  - 이미 연결 중이면 아무것도 하지 않는다.
   *  - 대기 시간(nextConnectAt) 중이면 접속하지 않는다.
   */
  connect(trigger = "unknown") {

    let result;

    try {

      result = this.openConnection(trigger);

    } catch (error) {

      this.status.lastError =
        "connect 예외: " + errMsg(error);

      this.status.lastAction =
        "Connect exception";

      this.failCount += 1;

      this.holdUntil(
        Date.now() + this.computeBackoff()
      );

      result = "error";

    }

    this.status.lastConnectCheck =
      trigger +
      " -> " +
      result +
      " @ " +
      new Date().toISOString();

    return result;

  }


  openConnection(trigger) {

    const now = Date.now();


    /*
     * ------------------------------------------------------
     * 1) 이미 소켓이 있으면 새로 만들지 않는다.
     * ------------------------------------------------------
     */
    if (this.socket) {

      const socket = this.socket;
      const state = socket.readyState;


      /* 닫히는 중 / 닫힌 소켓 → 정리하고 대기 시간 적용 */
      if (
        state === WebSocket.CLOSING ||
        state === WebSocket.CLOSED
      ) {

        this.handleDisconnect(
          socket,
          1006,
          "socket " + this.getSocketState()
        );

        return "cleaned_closed_socket";

      }


      /* READY 가 되지 않은 채 너무 오래 멈춰 있는 소켓 */
      if (
        !this.ready &&
        now - this.socketCreatedAt > CONNECT_TIMEOUT_MS
      ) {

        this.status.lastError =
          "Gateway READY 대기 시간 초과";

        this.dropSocket(
          socket,
          4000,
          "Connect timeout"
        );

        return "dropped_stuck_socket";

      }


      return "already_connected";

    }


    /*
     * ------------------------------------------------------
     * 2) Discord Bot Token 확인
     * ------------------------------------------------------
     */
    const token =
      this.env.AI_coupang_discord;

    if (!token) {

      this.status.lastError =
        "AI_coupang_discord 환경변수가 없습니다.";

      this.status.lastAction =
        "Discord token missing";

      this.status.gateway =
        "token_missing";

      return "token_missing";

    }


    /*
     * ------------------------------------------------------
     * 3) 대기 시간 확인
     *    (백오프 / 복구 불가능 오류 쿨다운 / 속도 제한)
     * ------------------------------------------------------
     */
    if (now < this.nextConnectAt) {

      if (!this.reconnectTimer) {

        this.armReconnectTimer();

      }

      return "waiting";

    }


    /*
     * ------------------------------------------------------
     * 4) Resume 가능 여부 / Identify 보호
     * ------------------------------------------------------
     */
    const resume = this.canResume();

    if (!resume) {

      const wait = this.identifyWaitMs(now);

      if (wait > 0) {

        this.status.gateway =
          "identify_limited";

        this.status.lastAction =
          "Identify limit: wait " +
          String(Math.ceil(wait / 1000)) +
          "s";

        this.holdUntil(now + wait);

        return "identify_limited";

      }

    }


    /*
     * ------------------------------------------------------
     * 5) WebSocket 생성
     * ------------------------------------------------------
     */
    if (!resume) {

      this.clearSession();

    }

    const gatewayUrl =
      resume
        ? this.resumeUrl.replace(/\/+$/, "") +
          "/?v=10&encoding=json"
        : DISCORD_GATEWAY;

    this.status.gateway = "connecting";

    this.status.lastAction =
      resume
        ? "Starting Discord Gateway connection (resume)"
        : "Starting Discord Gateway connection (identify)";


    let socket;

    try {

      socket = new WebSocket(gatewayUrl);

    } catch (error) {

      this.status.gateway =
        "connection_failed";

      this.status.lastAction =
        "Discord Gateway connection failed";

      this.status.lastError =
        errMsg(error);

      this.failCount += 1;

      this.holdUntil(
        now + this.computeBackoff()
      );

      return "connection_failed";

    }


    this.socket = socket;
    this.socketCreatedAt = now;

    this.connectMode =
      resume ? "resume" : "identify";

    this.helloHandled = false;
    this.ready = false;
    this.readyAt = 0;
    this.heartbeatAcked = true;

    this.status.lastAction =
      "WebSocket created";


    /*
     * 모든 핸들러는 "지금 이 소켓이 현재 소켓인지" 먼저 확인한다.
     * (오래된 소켓의 늦은 이벤트가 새 소켓을 망가뜨리지 못하게)
     */

    /* WebSocket OPEN */
    socket.addEventListener(
      "open",
      () => {

        if (this.socket !== socket) {
          return;
        }

        this.status.gateway = "connected";
        this.status.lastAction = "Discord WebSocket OPEN";

      }
    );


    /* Gateway 메시지 */
    socket.addEventListener(
      "message",
      event => {

        if (this.socket !== socket) {
          return;
        }

        this.handleGatewayMessage(
          socket,
          event.data
        );

      }
    );


    /* WebSocket 종료 (재접속 예약은 handleDisconnect 에서 한 번만) */
    socket.addEventListener(
      "close",
      event => {

        this.handleDisconnect(
          socket,
          event.code,
          event.reason
        );

      }
    );


    /* WebSocket 오류 */
    socket.addEventListener(
      "error",
      () => {

        if (this.socket !== socket) {
          return;
        }

        this.status.lastError =
          "Discord WebSocket error";

        if (
          socket.readyState === WebSocket.CLOSING ||
          socket.readyState === WebSocket.CLOSED
        ) {

          this.handleDisconnect(
            socket,
            1006,
            "WebSocket error"
          );

        }

      }
    );


    /* 접속 후 READY 가 오지 않으면 끊고 대기 후 재접속 */
    setTimeout(
      () => {

        if (
          this.socket === socket &&
          !this.ready
        ) {

          this.status.lastError =
            "Gateway READY 대기 시간 초과";

          this.dropSocket(
            socket,
            4000,
            "Connect timeout"
          );

        }

      },
      CONNECT_TIMEOUT_MS
    );


    return "connecting";

  }


  /*
   * ==========================================================
   * 연결 종료 처리 (재접속 예약은 여기서만 한다)
   * ==========================================================
   */
  handleDisconnect(socket, code, reason) {

    /* 이미 처리된 소켓 / 오래된 소켓의 이벤트는 무시 */
    if (this.socket !== socket) {
      return;
    }


    const now = Date.now();

    const wasStable =
      this.ready &&
      now - this.readyAt >= STABLE_CONNECTION_MS;


    this.socket = null;
    this.ready = false;
    this.helloHandled = false;

    this.clearHeartbeat();


    /* 오래 안정적으로 쓰던 연결이면 실패 횟수 초기화, 아니면 증가 */
    if (wasStable) {

      this.failCount = 0;

    } else {

      this.failCount += 1;

    }


    const closeCode =
      Number(code) || 1006;

    const info =
      CLOSE_CODE_INFO[closeCode] || "";


    this.status.gateway = "closed";

    this.status.lastAction =
      "Gateway CLOSED: " +
      String(closeCode);

    this.status.lastEvent =
      "CLOSE code=" +
      String(closeCode) +
      ", reason=" +
      String(reason || "none");

    this.status.lastEventTime =
      new Date(now).toISOString();


    /* 세션이 무효가 되는 코드 */
    if (RESET_SESSION_CODES.has(closeCode)) {

      this.clearSession();

    }


    /* 다음 접속까지 대기 시간 */
    let delay =
      this.computeBackoff();


    if (FATAL_CLOSE_CODES.has(closeCode)) {

      /* 토큰 / Intent 오류 등: 재시도해도 소용없으므로 길게 쉰다. */
      this.clearSession();

      delay =
        Math.max(delay, FATAL_COOLDOWN_MS);

      this.status.gateway = "fatal";

      this.status.lastError =
        "복구할 수 없는 종료 코드 " +
        String(closeCode) +
        (info ? " (" + info + ")" : "") +
        " → " +
        String(Math.round(delay / 60000)) +
        "분 후 다시 시도합니다.";

    } else if (closeCode === 4008) {

      delay =
        Math.max(delay, RATE_LIMIT_COOLDOWN_MS);

    }


    this.holdUntil(now + delay);

  }


  /*
   * 우리가 직접 소켓을 끊을 때 사용.
   * close 이벤트를 기다리지 않고 바로 정리하므로,
   * 이후에 도착하는 close 이벤트는 무시된다.
   */
  dropSocket(socket, code, reason) {

    try {

      socket.close(code, reason);

    } catch {}

    this.handleDisconnect(
      socket,
      code,
      reason
    );

  }


  /*
   * ==========================================================
   * 재접속 대기 / 타이머
   * ==========================================================
   */

  /* timestamp 이전에는 접속하지 않는다. (대기 시간은 늘어나기만 한다.) */
  holdUntil(timestamp) {

    if (timestamp > this.nextConnectAt) {

      this.nextConnectAt = timestamp;

    }

    this.armReconnectTimer();

  }


  armReconnectTimer() {

    if (this.reconnectTimer) {

      clearTimeout(this.reconnectTimer);

      this.reconnectTimer = null;

    }

    const wait =
      Math.max(0, this.nextConnectAt - Date.now()) + 100;

    this.reconnectTimer = setTimeout(
      () => {

        this.reconnectTimer = null;

        this.connect("timer");

      },
      wait
    );

  }


  /* 5초 → 10초 → 20초 → ... 최대 5분 (+ 약간의 랜덤) */
  computeBackoff() {

    if (this.failCount <= 0) {

      return RECONNECT_FIRST_MS;

    }

    const exp =
      Math.min(
        RECONNECT_MAX_MS,
        RECONNECT_BASE_MS *
          Math.pow(2, this.failCount - 1)
      );

    const jitter =
      Math.random() *
      Math.min(exp * 0.25, 10 * 1000);

    return Math.floor(exp + jitter);

  }


  /*
   * ==========================================================
   * 세션 / Identify 보호
   * ==========================================================
   */
  canResume() {

    return Boolean(
      this.sessionId &&
      this.resumeUrl &&
      this.sequence !== null
    );

  }


  clearSession() {

    this.sessionId = null;
    this.resumeUrl = null;
    this.sequence = null;

  }


  /* Identify 를 보내기 전에 더 기다려야 하는 시간(ms) */
  identifyWaitMs(now) {

    this.identifyLog = this.identifyLog.filter(
      t => now - t < DAY_MS
    );

    let wait = 0;

    const last =
      this.identifyLog[this.identifyLog.length - 1];

    if (
      last !== undefined &&
      now - last < IDENTIFY_MIN_GAP_MS
    ) {

      wait = IDENTIFY_MIN_GAP_MS - (now - last);

    }

    if (this.identifyLog.length >= IDENTIFY_DAILY_LIMIT) {

      wait = Math.max(
        wait,
        this.identifyLog[0] + DAY_MS - now
      );

    }

    return wait;

  }


  recordIdentify() {

    const now = Date.now();

    this.identifyLog = this.identifyLog.filter(
      t => now - t < DAY_MS
    );

    this.identifyLog.push(now);

    try {

      this.state.storage
        .put("identifyLog", [...this.identifyLog])
        .catch(error => {

          this.status.lastError =
            "identifyLog 저장 실패: " +
            errMsg(error);

        });

    } catch (error) {

      this.status.lastError =
        "identifyLog 저장 실패: " +
        errMsg(error);

    }

  }


  /*
   * ==========================================================
   * Discord Gateway 메시지 처리
   * ==========================================================
   */
  handleGatewayMessage(socket, rawData) {

    let payload;

    try {

      const text =
        typeof rawData === "string"
          ? rawData
          : new TextDecoder().decode(rawData);

      payload = JSON.parse(text);

    } catch (error) {

      this.status.lastError =
        "Gateway JSON 파싱 실패: " +
        errMsg(error);

      return;

    }


    const op = payload.op;
    const d = payload.d;
    const s = payload.s;
    const t = payload.t;


    /* Sequence 저장 (Heartbeat / Resume 에 사용) */
    if (typeof s === "number") {

      this.sequence = s;

    }


    if (op !== 11) {

      this.status.lastEvent =
        "op=" +
        String(op) +
        ", t=" +
        String(t || "NONE");

      this.status.lastEventTime =
        new Date().toISOString();

    }


    /*
     * ======================================================
     * HELLO
     * ======================================================
     */
    if (op === 10) {

      if (this.helloHandled) {
        return;
      }

      this.helloHandled = true;

      const heartbeatInterval =
        d && d.heartbeat_interval
          ? d.heartbeat_interval
          : 41250;

      this.status.lastAction =
        "Discord Gateway HELLO received";

      this.startHeartbeat(
        socket,
        heartbeatInterval
      );

      if (this.connectMode === "resume") {

        this.sendResume(socket);

      } else {

        this.sendIdentify(socket);

      }

      return;

    }


    /*
     * ======================================================
     * Heartbeat ACK
     * ======================================================
     */
    if (op === 11) {

      this.heartbeatAcked = true;

      this.status.lastHeartbeatAckAt =
        new Date().toISOString();

      return;

    }


    /*
     * ======================================================
     * Discord 가 Heartbeat 를 바로 보내달라고 요청
     * ======================================================
     */
    if (op === 1) {

      this.sendHeartbeat(socket, true);

      return;

    }


    /*
     * ======================================================
     * Discord Reconnect 요청 (세션은 유지 → Resume)
     * ======================================================
     */
    if (op === 7) {

      this.status.lastAction =
        "Discord requested reconnect";

      this.dropSocket(
        socket,
        4000,
        "Discord requested reconnect"
      );

      return;

    }


    /*
     * ======================================================
     * Invalid Session
     * ======================================================
     *
     * d === true  : Resume 가능
     * d === false : 세션 폐기 → 새로 Identify
     * 두 경우 모두 공통 대기 시간(백오프)을 지킨 뒤 재접속한다.
     */
    if (op === 9) {

      const resumable = d === true;

      this.status.lastAction =
        "Discord Invalid Session (resumable=" +
        String(resumable) +
        ")";

      if (!resumable) {

        this.clearSession();

      }

      this.dropSocket(
        socket,
        resumable ? 4000 : 1000,
        "Invalid session"
      );

      return;

    }


    /*
     * ======================================================
     * Dispatch
     * ======================================================
     */
    if (op === 0) {

      /* READY */
      if (t === "READY") {

        this.sessionId =
          d && d.session_id
            ? d.session_id
            : null;

        this.resumeUrl =
          d && d.resume_gateway_url
            ? d.resume_gateway_url
            : "wss://gateway.discord.gg";

        this.botUserId =
          d && d.user && d.user.id
            ? String(d.user.id)
            : null;

        this.markReady("Discord bot READY");

        return;

      }


      /* RESUMED */
      if (t === "RESUMED") {

        this.markReady("Discord session RESUMED");

        return;

      }


      /* MESSAGE_CREATE */
      if (t === "MESSAGE_CREATE") {

        this.handleMessageCreate(d).catch(
          error => {

            this.status.lastError =
              "메시지 처리 실패: " +
              errMsg(error);

          }
        );

        return;

      }

    }

  }


  markReady(action) {

    const socket = this.socket;

    this.ready = true;
    this.readyAt = Date.now();

    this.status.gateway = "ready";
    this.status.lastAction = action;
    this.status.lastError = null;

    /* 오래 유지되면 실패 횟수를 0으로 되돌린다. */
    setTimeout(
      () => {

        if (
          this.socket === socket &&
          this.ready
        ) {

          this.failCount = 0;

        }

      },
      STABLE_CONNECTION_MS
    );

  }


  /*
   * ==========================================================
   * Heartbeat
   * ==========================================================
   */
  startHeartbeat(socket, interval) {

    this.clearHeartbeat();

    this.heartbeatAcked = true;

    this.status.lastAction =
      "Heartbeat started: " +
      String(interval) +
      "ms";

    /* Discord 권장: 첫 Heartbeat 는 interval × (0~1) 만큼 기다린 뒤 전송 */
    const firstDelay =
      Math.floor(interval * Math.random());

    this.heartbeatStartTimer = setTimeout(
      () => {

        this.heartbeatStartTimer = null;

        if (this.socket !== socket) {
          return;
        }

        this.sendHeartbeat(socket);

        if (this.socket !== socket) {
          return;
        }

        this.heartbeatTimer = setInterval(
          () => {

            this.sendHeartbeat(socket);

          },
          interval
        );

      },
      firstDelay
    );

  }


  clearHeartbeat() {

    if (this.heartbeatStartTimer) {

      clearTimeout(this.heartbeatStartTimer);

      this.heartbeatStartTimer = null;

    }

    if (this.heartbeatTimer) {

      clearInterval(this.heartbeatTimer);

      this.heartbeatTimer = null;

    }

  }


  sendHeartbeat(socket, force = false) {

    if (
      this.socket !== socket ||
      socket.readyState !== WebSocket.OPEN
    ) {
      return;
    }


    /*
     * 지난 Heartbeat 에 ACK 가 없었다면 연결이 끊긴 것이다.
     * → 끊고 (Resume 가능한 코드 4000), 공통 대기 시간 후 재접속
     */
    if (
      !force &&
      !this.heartbeatAcked
    ) {

      this.status.lastError =
        "Heartbeat ACK 없음 → 연결을 끊고 재접속합니다.";

      this.status.lastAction =
        "Heartbeat ACK missing";

      this.dropSocket(
        socket,
        4000,
        "Heartbeat ACK missing"
      );

      return;

    }


    if (
      this.safeSend(
        socket,
        {
          op: 1,
          d: this.sequence
        }
      )
    ) {

      this.heartbeatAcked = false;

      this.status.lastHeartbeatAt =
        new Date().toISOString();

    }

  }


  /*
   * ==========================================================
   * Gateway 전송
   * ==========================================================
   */
  safeSend(socket, data) {

    if (
      this.socket !== socket ||
      socket.readyState !== WebSocket.OPEN
    ) {
      return false;
    }

    try {

      socket.send(JSON.stringify(data));

      return true;

    } catch (error) {

      this.status.lastError =
        "Gateway 전송 실패: " +
        errMsg(error);

      return false;

    }

  }


  /* Discord Identify */
  sendIdentify(socket) {

    const token =
      this.env.AI_coupang_discord;

    if (!token) {

      this.status.lastError =
        "AI_coupang_discord 환경변수가 없습니다.";

      return;

    }

    const sent = this.safeSend(
      socket,
      {
        op: 2,

        d: {

          token: token,

          intents: INTENTS,

          properties: {
            os: "linux",
            browser: "cloudflare-worker",
            device: "cloudflare-worker"
          }

        }
      }
    );

    if (sent) {

      this.recordIdentify();

      this.status.lastAction =
        "Discord Identify sent";

    }

  }


  /* Discord Resume (끊기기 전 세션을 이어서 사용) */
  sendResume(socket) {

    const token =
      this.env.AI_coupang_discord;

    const sent = this.safeSend(
      socket,
      {
        op: 6,

        d: {
          token: token,
          session_id: this.sessionId,
          seq: this.sequence
        }
      }
    );

    if (sent) {

      this.status.lastAction =
        "Discord Resume sent";

    }

  }


  /*
   * ==========================================================
   * Discord 메시지 처리
   * ==========================================================
   */
  isDuplicateMessage(messageId) {

    if (!messageId) {
      return false;
    }

    if (this.seenMessageIds.has(messageId)) {
      return true;
    }

    this.seenMessageIds.add(messageId);

    if (this.seenMessageIds.size > SEEN_MESSAGE_LIMIT) {

      const oldest =
        this.seenMessageIds.values().next().value;

      this.seenMessageIds.delete(oldest);

    }

    return false;

  }


  async handleMessageCreate(message) {

    if (!message) {
      return;
    }


    /* 봇 자신(및 다른 봇)의 메시지는 무시 */
    if (
      message.author &&
      message.author.bot
    ) {
      return;
    }


    const devChannel =
      this.env.AI_coupang_discord_dev_channel;

    const userChannel =
      this.env.AI_coupang_discord_user_channel;

    const channelId =
      String(message.channel_id);


    /* 지정된 채널만 처리 */
    if (
      channelId !== String(devChannel) &&
      channelId !== String(userChannel)
    ) {
      return;
    }


    const content =
      message.content
        ? message.content.trim()
        : "";

    if (!content) {
      return;
    }


    /* 같은 메시지를 두 번 처리하지 않는다. (Resume 재전송 / 중복 세션 대비) */
    if (this.isDuplicateMessage(message.id)) {
      return;
    }


    this.status.lastAction =
      "Processing Discord message";


    try {

      /* OpenRouter 요청 */
      const answer =
        await this.askOpenRouter(content);

      if (!answer) {

        throw new Error(
          "OpenRouter 응답이 비어 있습니다."
        );

      }

      /* Discord 응답 */
      await this.sendDiscordMessage(
        channelId,
        answer
      );

      this.status.lastAction =
        "AI response sent to Discord";

    } catch (error) {

      const errorMessage = errMsg(error);

      this.status.lastError =
        "AI 처리 실패: " + errorMessage;

      try {

        await this.sendDiscordMessage(
          channelId,
          "AI 처리 중 오류가 발생했습니다.\n" +
          "오류: " +
          errorMessage
        );

      } catch (sendError) {

        this.status.lastError =
          "Discord 오류 메시지 전송 실패: " +
          errMsg(sendError);

      }

    }

  }


  /*
   * ==========================================================
   * OpenRouter API
   * ==========================================================
   */
  async askOpenRouter(userMessage) {

    const apiKey =
      this.env.AI_coupang_api;

    if (!apiKey) {

      throw new Error(
        "AI_coupang_api 환경변수가 없습니다."
      );

    }


    const endpoint =
      "https://openrouter.ai/api/v1/chat/completions";


    const response =
      await fetch(
        endpoint,
        {

          method: "POST",

          headers: {

            "Content-Type":
              "application/json",

            "Authorization":
              "Bearer " + apiKey,

            "HTTP-Referer":
              "https://coupang-ai-server.yjh20130103.workers.dev",

            "X-Title":
              "Coupang AI Discord Bot"

          },

          body: JSON.stringify(
            {

              model: OPENROUTER_MODEL,

              messages: [
                {
                  role: "user",
                  content: userMessage
                }
              ],

              temperature: 0.7,
              top_p: 0.9,
              max_tokens: 1000

            }
          )

        }
      );


    const text =
      await response.text();


    if (!response.ok) {

      throw new Error(
        "OpenRouter API " +
        String(response.status) +
        ": " +
        text
      );

    }


    let data;

    try {

      data = JSON.parse(text);

    } catch {

      throw new Error(
        "OpenRouter 응답 JSON 파싱 실패"
      );

    }


    if (
      !data ||
      !data.choices ||
      !data.choices[0] ||
      !data.choices[0].message
    ) {

      throw new Error(
        "OpenRouter가 답변을 반환하지 않았습니다."
      );

    }


    let answer =
      data.choices[0].message.content;


    if (Array.isArray(answer)) {

      answer =
        answer
          .map(
            part => {

              if (typeof part === "string") {
                return part;
              }

              if (part && part.text) {
                return part.text;
              }

              return "";

            }
          )
          .join("");

    }


    answer =
      typeof answer === "string"
        ? answer.trim()
        : "";


    if (!answer) {

      throw new Error(
        "OpenRouter가 답변을 반환하지 않았습니다."
      );

    }


    return answer;

  }


  /*
   * ==========================================================
   * Discord 메시지 전송
   * ==========================================================
   */
  async sendDiscordMessage(channelId, content) {

    const token =
      this.env.AI_coupang_discord;

    if (!token) {

      throw new Error(
        "AI_coupang_discord 환경변수가 없습니다."
      );

    }


    /* Discord 메시지는 최대 2000자 */
    const chunks =
      this.splitMessage(content, 1900);


    for (const chunk of chunks) {

      await this.postDiscordMessage(
        channelId,
        token,
        chunk
      );

    }

  }


  async postDiscordMessage(channelId, token, content) {

    const endpoint =
      DISCORD_API +
      "/channels/" +
      channelId +
      "/messages";


    for (let attempt = 0; attempt < 3; attempt++) {

      const response =
        await fetch(
          endpoint,
          {

            method: "POST",

            headers: {
              "Authorization": "Bot " + token,
              "Content-Type": "application/json"
            },

            body: JSON.stringify(
              {
                content: content
              }
            )

          }
        );


      if (response.ok) {
        return;
      }


      /* 속도 제한(429): Discord 가 알려준 시간만큼 기다렸다가 다시 시도 */
      if (
        response.status === 429 &&
        attempt < 2
      ) {

        let waitMs = 1000;

        try {

          const data = await response.json();

          if (
            data &&
            typeof data.retry_after === "number"
          ) {

            waitMs = Math.ceil(data.retry_after * 1000);

          }

        } catch {}

        await sleep(
          Math.min(Math.max(waitMs, 500), 15000)
        );

        continue;

      }


      const errorText =
        await response.text();

      throw new Error(
        "Discord API " +
        String(response.status) +
        ": " +
        errorText
      );

    }

  }


  /*
   * ==========================================================
   * 긴 메시지 분할
   * ==========================================================
   */
  splitMessage(text, maxLength) {

    if (text.length <= maxLength) {

      return [text];

    }


    const chunks = [];

    let remaining = text;


    while (remaining.length > maxLength) {

      let cut =
        remaining.lastIndexOf("\n", maxLength);

      if (cut < 500) {

        cut =
          remaining.lastIndexOf(" ", maxLength);

      }

      if (cut < 1) {

        cut = maxLength;

      }

      chunks.push(
        remaining.slice(0, cut)
      );

      remaining =
        remaining
          .slice(cut)
          .trimStart();

    }


    if (remaining.length > 0) {

      chunks.push(remaining);

    }


    return chunks;

  }

}
