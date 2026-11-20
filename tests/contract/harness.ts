import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash, createHmac, generateKeyPairSync, type KeyObject, randomUUID, sign, verify } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type IncomingMessage, type Server as HttpServer, type ServerResponse } from "node:http";
import { createServer as createTcpServer, type Server as TcpServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { Novu } from "@novu/api";
import { Client, NovuRequestHandler } from "@novu/framework";
import { Novu as Inbox } from "@novu/js";
import { workflows } from "./workflows.ts";

const ROOT = new URL("../..", import.meta.url).pathname;
export const APP_ID = "contract-main";
export const BRIDGE_PATH = "/api/novu";
export const BRIDGE_LESS_PATH = "/api/novu-less";

export const envId = (name: string) => createHash("sha256").update(name).digest("hex").slice(0, 24);
export const hmac = (key: string, text: string) => createHmac("sha256", key).update(text).digest("hex");
export const sid = () => `sub-${randomUUID()}`;
export const uid = (prefix: string) => `${prefix}-${randomUUID()}`;

export function at(value: unknown, ...path: string[]): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== "object" || current === null) return undefined;
    current = Object.getOwnPropertyDescriptor(current, key)?.value;
  }
  return current;
}

const record = (value: unknown): { [key: string]: unknown } =>
  typeof value === "object" && value !== null ? Object.fromEntries(Object.entries(value)) : {};

function parse(raw: string): unknown {
  try {
    return JSON.parse(raw);
  } catch {
    // ignored: callers treat undefined as a body that is not JSON
    return undefined;
  }
}

function binary(): string {
  if (process.env.NANOVU_BIN) return process.env.NANOVU_BIN;
  const meta = parse(execFileSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], { cwd: ROOT }).toString());
  return join(String(at(meta, "target_directory")), "debug", "nanovu");
}

async function listen(server: HttpServer | TcpServer): Promise<number> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return Number(at(server.address(), "port"));
}

async function freePort() {
  const server = createTcpServer();
  const port = await listen(server);
  server.close();
  await once(server, "close");
  return port;
}

async function readBody(req: IncomingMessage) {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks).toString();
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

export async function waitFor<T>(
  fn: () => T | Promise<T>,
  { timeout = 5000, interval = 50, what = "condition" } = {},
): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeout;
  let last: unknown;
  for (;;) {
    try {
      const value = await fn();
      if (value) return value;
      last = value;
    } catch (err) {
      last = err;
    }
    if (Date.now() > deadline) {
      const detail = last instanceof Error ? last.message : JSON.stringify(last);
      throw new Error(`timed out after ${timeout} ms waiting for ${what}; last: ${detail}`);
    }
    await sleep(interval);
  }
}

export async function expectError<E extends Error & { statusCode: number }>(
  promise: Promise<unknown>,
  Class: abstract new (...args: never[]) => E,
  status: number,
): Promise<E> {
  let err: unknown;
  try {
    await promise;
  } catch (e) {
    err = e;
  }
  assert.ok(err, `expected ${Class.name} ${status}, call succeeded`);
  const got = err instanceof Error ? `${err.name} ${at(err, "statusCode") ?? ""}: ${err.message}` : String(err);
  assert.ok(err instanceof Class && err.statusCode === status, `expected ${Class.name} ${status}, got ${got}`);
  return err;
}

type Outcome<D> = { data?: D; error?: { message: string; originalError?: { message?: string } } };

export function unwrap<D>(result: Outcome<D>): D;
export function unwrap<D>(result: Outcome<D>): D | undefined {
  if (result.error) {
    const cause = result.error.originalError?.message ?? "";
    throw new Error(`${result.error.message}${cause ? `: ${cause}` : ""}`);
  }
  return result.data;
}

type BridgeRequest = { method?: string; action: string | null; raw: string };

function bridgeServer(secretKey: string) {
  const handler = (set: typeof workflows) =>
    new NovuRequestHandler({
      frameworkName: "nanovu-contract",
      workflows: set,
      client: new Client({ secretKey, strictAuthentication: true, verbose: false }),
      handler: (req: IncomingMessage, res: ServerResponse, body: unknown) => {
        const url = new URL(req.url ?? "/", `http://${req.headers.host}`);
        return {
          body: () => body,
          headers: (name: string) => req.headers[name]?.toString(),
          method: () => req.method ?? "",
          url: () => url,
          queryString: (name: string) => url.searchParams.get(name),
          transformResponse: ({ status, headers, body: out }: { status: number; headers: Record<string, string>; body: string }) => {
            res.writeHead(status, headers);
            res.end(out);
          },
        };
      },
    }).createHandler();
  const routes = new Map([
    [BRIDGE_PATH, handler(workflows)],
    [BRIDGE_LESS_PATH, handler(workflows.filter((w) => w.id !== "email"))],
  ]);
  const requests: BridgeRequest[] = [];
  const server = createServer(async (req, res) => {
    const raw = await readBody(req);
    const url = new URL(req.url ?? "/", "http://bridge");
    requests.push({ method: req.method, action: url.searchParams.get("action"), raw });
    const route = routes.get(url.pathname);
    if (!route) return sendJson(res, 404, { message: "no route" });
    await route(req, res, raw ? (parse(raw) ?? {}) : {});
  });
  return { requests, server };
}

type FcmMessage = {
  token: unknown;
  notification: { title: unknown; body: unknown };
  data: { [key: string]: unknown };
};
type FcmSend = { path: string; authorization?: string; message: FcmMessage };
type TokenGrant = { form: { [key: string]: string }; claims: { [key: string]: unknown }; valid: boolean };

function googleFake(publicKey: KeyObject) {
  const tokens: TokenGrant[] = [];
  const sends: FcmSend[] = [];
  const unregistered = new Set<unknown>();
  const server = createServer(async (req, res) => {
    const raw = await readBody(req);
    const path = req.url ?? "";
    if (req.method === "POST" && path === "/token") {
      const form = Object.fromEntries(new URLSearchParams(raw));
      const [head, claims, sig] = (form.assertion ?? "").split(".");
      let valid = false;
      let decoded: unknown = null;
      try {
        valid = verify("sha256", Buffer.from(`${head}.${claims}`), publicKey, Buffer.from(sig, "base64url"));
        decoded = parse(Buffer.from(claims, "base64url").toString());
      } catch {
        // ignored: a malformed assertion stays invalid and gets invalid_grant below
      }
      tokens.push({ form, claims: record(decoded), valid });
      if (!valid) return sendJson(res, 400, { error: "invalid_grant" });
      return sendJson(res, 200, { access_token: "fake-access-token", expires_in: 3600, token_type: "Bearer" });
    }
    if (req.method === "POST" && path.endsWith("/messages:send")) {
      const sent = at(parse(raw), "message");
      const message = {
        token: at(sent, "token"),
        notification: { title: at(sent, "notification", "title"), body: at(sent, "notification", "body") },
        data: record(at(sent, "data")),
      };
      sends.push({ path, authorization: req.headers.authorization, message });
      if (unregistered.has(message.token)) {
        return sendJson(res, 404, {
          error: {
            code: 404,
            message: "Requested entity was not found.",
            status: "NOT_FOUND",
            details: [{ "@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", errorCode: "UNREGISTERED" }],
          },
        });
      }
      return sendJson(res, 200, { name: `projects/contract/messages/${randomUUID()}` });
    }
    sendJson(res, 404, { error: { code: 404, message: `no route ${req.method} ${path}` } });
  });
  return { tokens, sends, unregistered, server };
}

type Mail = { from: string; to: string[]; data: string };

function smtpSink() {
  const mails: Mail[] = [];
  const server = createTcpServer((sock) => {
    let buf = "";
    let inData = false;
    let mail: Mail = { from: "", to: [], data: "" };
    const reply = (line: string) => sock.write(`${line}\r\n`);
    sock.on("error", () => {});
    sock.on("data", (chunk) => {
      buf += chunk.toString("latin1");
      for (;;) {
        if (inData) {
          const end = buf.indexOf("\r\n.\r\n");
          if (end < 0) return;
          mail.data = buf.slice(0, end);
          buf = buf.slice(end + 5);
          inData = false;
          mails.push(mail);
          mail = { from: "", to: [], data: "" };
          reply("250 OK queued");
          continue;
        }
        const eol = buf.indexOf("\r\n");
        if (eol < 0) return;
        const line = buf.slice(0, eol);
        buf = buf.slice(eol + 2);
        const verb = line.split(" ")[0].toUpperCase();
        if (verb === "EHLO") reply("250-sink\r\n250-8BITMIME\r\n250 SMTPUTF8");
        else if (verb === "HELO") reply("250 sink");
        else if (verb === "MAIL") {
          mail.from = line.match(/<(.*)>/)?.[1] ?? "";
          reply("250 OK");
        } else if (verb === "RCPT") {
          mail.to.push(line.match(/<(.*)>/)?.[1] ?? "");
          reply("250 OK");
        } else if (verb === "DATA") {
          inData = true;
          buf = `\r\n${buf}`;
          reply("354 end with <CRLF>.<CRLF>");
        } else if (verb === "QUIT") {
          sock.end("221 bye\r\n");
          return;
        } else if (verb === "RSET" || verb === "NOOP") reply("250 OK");
        else reply("502 not implemented");
      }
    });
    reply("220 sink ESMTP");
  });
  return { mails, server };
}

export type Ctx = Awaited<ReturnType<typeof start>>;

export async function start() {
  const dir = mkdtempSync(join(tmpdir(), "nanovu-contract-"));
  const secretFile = (name: string, value: string) => {
    const path = join(dir, name);
    writeFileSync(path, value, { mode: 0o600 });
    return path;
  };
  const secretKey = `main-${randomUUID()}`;
  const bareKey = `bare-${randomUUID()}`;
  const { publicKey, privateKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });

  const bridge = bridgeServer(secretKey);
  const google = googleFake(publicKey);
  const smtp = smtpSink();
  const [bridgePort, googlePort, smtpPort, port] = await Promise.all([
    listen(bridge.server),
    listen(google.server),
    listen(smtp.server),
    freePort(),
  ]);
  const googleUrl = `http://127.0.0.1:${googlePort}`;
  const serviceAccount = {
    type: "service_account",
    project_id: "contract",
    private_key_id: "contract-key",
    private_key: privateKey.export({ type: "pkcs8", format: "pem" }).toString(),
    client_email: "nanovu@contract.iam.gserviceaccount.com",
    client_id: "1",
    token_uri: `${googleUrl}/token`,
  };
  const s = JSON.stringify;
  const config = `listen = ${s(`127.0.0.1:${port}`)}
database = ${s(join(dir, "nanovu.db"))}

[environments.main]
application_identifier = ${s(APP_ID)}
secret_key_file = ${s(secretFile("main-key", secretKey))}
bridge_hosts = ["127.0.0.1"]

[environments.main.integrations.push]
provider = "fcm"
service_account_file = ${s(secretFile("fcm.json", JSON.stringify(serviceAccount)))}
endpoint = ${s(googleUrl)}

[environments.main.integrations.mail]
provider = "smtp"
url_file = ${s(secretFile("smtp-url", `smtp://127.0.0.1:${smtpPort}`))}
from = "nanovu@contract.test"

[environments.bare]
application_identifier = "contract-bare"
secret_key_file = ${s(secretFile("bare-key", bareKey))}
`;
  const configPath = join(dir, "nanovu.toml");
  writeFileSync(configPath, config);

  let logs = "";
  const child = spawn(binary(), ["serve", "--config", configPath], { stdio: ["ignore", "ignore", "pipe"] });
  child.stderr.on("data", (chunk) => {
    logs = (logs + chunk).slice(-65536);
  });
  let exited: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  child.on("exit", (code, signal) => {
    exited = { code, signal };
  });

  const base = `http://127.0.0.1:${port}`;
  try {
    await waitFor(
      async () => {
        if (exited) throw new Error(`nanovu exited ${JSON.stringify(exited)}:\n${logs}`);
        return (await fetch(`${base}/healthz`)).ok;
      },
      { timeout: 10000, what: "nanovu /healthz" },
    );
  } catch (err) {
    child.kill("SIGKILL");
    throw err;
  }

  process.env.NOVU_API_URL = base;
  process.env.NOVU_SECRET_KEY = secretKey;

  const socketUrl = `ws://127.0.0.1:${port}`;
  const api = new Novu({ secretKey, serverURL: base, retryConfig: { strategy: "none" } });
  const bridgeUrl = (path = BRIDGE_PATH) => `http://127.0.0.1:${bridgePort}${path}`;
  const hash = (subscriberId: string) => hmac(secretKey, subscriberId);
  const inboxes: Inbox[] = [];

  async function sync(path = BRIDGE_PATH) {
    const res = await fetch(`${base}/v1/bridge/sync`, {
      method: "POST",
      headers: { authorization: `ApiKey ${secretKey}`, "content-type": "application/json" },
      body: JSON.stringify({ bridgeUrl: bridgeUrl(path) }),
    });
    const text = await res.text();
    return { status: res.status, body: parse(text) ?? text };
  }

  function inbox(subscriberId: string, { hash: subscriberHash = hash(subscriberId) } = {}) {
    const novu = new Inbox({
      applicationIdentifier: APP_ID,
      subscriber: subscriberId,
      subscriberHash,
      apiUrl: base,
      socketUrl,
      socketOptions: { socketType: "cloud" },
      useCache: false,
    });
    inboxes.push(novu);
    return novu;
  }

  async function inAppMessages(subscriberId: string, transactionId?: string) {
    const { result } = await api.messages.retrieve({
      subscriberId,
      ...(transactionId ? { transactionId: [transactionId] } : {}),
      limit: 100,
    });
    return result.data.filter((m) => m.channel === "in_app");
  }

  function waitForInApp(subscriberId: string, transactionId?: string, timeout = 5000) {
    return waitFor(async () => (await inAppMessages(subscriberId, transactionId))[0], {
      timeout,
      what: `in_app message ${transactionId ?? ""} for ${subscriberId}`,
    });
  }

  async function settle() {
    const [subscriberId, transactionId] = [sid(), uid("tx")];
    await api.trigger({ workflowId: "alert", to: subscriberId, payload: { title: "Hello", body: "settle" }, transactionId });
    await waitForInApp(subscriberId, transactionId);
  }

  async function stop() {
    await Promise.allSettled(inboxes.map((n) => n.socket.disconnect()));
    child.kill("SIGTERM");
    if (!exited) await Promise.race([once(child, "exit"), sleep(3000)]);
    if (!exited) child.kill("SIGKILL");
    for (const server of [bridge.server, google.server, smtp.server]) server.close();
    bridge.server.closeAllConnections();
    google.server.closeAllConnections();
    rmSync(dir, { recursive: true, force: true });
  }

  return {
    base,
    socketUrl,
    secretKey,
    bridge,
    bridgeUrl,
    google,
    smtp,
    serviceAccount,
    logs: () => logs,
    api,
    bareApi: new Novu({ secretKey: bareKey, serverURL: base, retryConfig: { strategy: "none" } }),
    hash,
    sync,
    inbox,
    inAppMessages,
    settle,
    waitForInApp,
    pushesWithBody: (body: string) => google.sends.filter((r) => r.message.notification.body === body),
    stop,
  };
}

export function signature(secretKey: string, body: unknown, t = Date.now()) {
  return `t=${t},v1=${hmac(secretKey, `${t}.${JSON.stringify(body)}`)}`;
}

export function signJwt(privateKeyPem: string, claims: unknown) {
  const enc = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
  const unsigned = `${enc({ alg: "RS256", typ: "JWT" })}.${enc(claims)}`;
  return `${unsigned}.${sign("sha256", Buffer.from(unsigned), privateKeyPem).toString("base64url")}`;
}
