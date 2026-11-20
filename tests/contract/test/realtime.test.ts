import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { Events, Novu as Inbox } from "@novu/js";
import { sid, start, uid, type Ctx } from "../harness.ts";

let ctx: Ctx;
before(async () => {
  ctx = await start();
  await ctx.sync();
});
after(() => ctx?.stop());

const within = <T>(promise: Promise<T>, ms: number, what: string) =>
  Promise.race([
    promise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`no ${what} within ${ms} ms`)), ms).unref()),
  ]);

const connection = (novu: Inbox) =>
  new Promise<void>((resolve, reject) => {
    novu.on("socket.connect.resolved", ({ error }) => (error ? reject(error) : resolve()));
  });

function listen<K extends keyof Events>(novu: Inbox, event: K) {
  const queue: Events[K][] = [];
  const waiters: ((payload: Events[K]) => void)[] = [];
  novu.on(event, (payload) => {
    const waiter = waiters.shift();
    waiter ? waiter(payload) : queue.push(payload);
  });
  return (ms = 5000) => {
    const queued = queue.shift();
    return within(
      queued !== undefined ? Promise.resolve(queued) : new Promise<Events[K]>((r) => waiters.push(r)),
      ms,
      event,
    );
  };
}

async function trigger(id: string) {
  const transactionId = uid("tx");
  const body = uid("body");
  await ctx.api.trigger({
    workflowId: "alert",
    to: id,
    payload: { title: "Hello", body, data: { kind: "rt" } },
    transactionId,
  });
  return { transactionId, body };
}

test("realtime: notification_received is mapped by the SDK", async () => {
  const id = sid();
  const novu = ctx.inbox(id);
  const connected = connection(novu);
  const received = listen(novu, "notifications.notification_received");
  const unseenChanged = listen(novu, "notifications.unseen_count_changed");
  await within(connected, 5000, "socket connection");
  const { transactionId, body } = await trigger(id);
  const { result: n } = await received();
  assert.equal(n.transactionId, transactionId);
  assert.equal(n.subject, "Hello");
  assert.equal(n.body, body);
  assert.equal(n.isRead, false);
  assert.equal(n.isSeen, false);
  assert.equal(n.isArchived, false);
  assert.equal(n.channelType, "in_app");
  assert.deepEqual(n.tags, ["Activity"]);
  assert.deepEqual(n.data, { kind: "rt" });
  assert.equal(n.primaryAction?.label, "Open");
  assert.equal(n.primaryAction?.redirect?.url, "/open");
  assert.equal(n.redirect?.url, "/r");
  assert.equal(n.workflow?.identifier, "alert");
  assert.equal(typeof n.id, "string");
  const { result: unseen } = await unseenChanged();
  assert.equal(unseen, 1);
});

test("realtime: unread count changes on new message, server mark-as and delete-by-transaction", async () => {
  const id = sid();
  const novu = ctx.inbox(id);
  const connected = connection(novu);
  const next = listen(novu, "notifications.unread_count_changed");
  await within(connected, 5000, "socket connection");

  const { transactionId } = await trigger(id);
  const created = (await next()).result;
  assert.equal(created.total, 1);
  assert.deepEqual(Object.keys(created.severity).sort(), ["high", "low", "medium", "none"]);

  const message = await ctx.waitForInApp(id, transactionId);
  assert.ok(message.id);
  await ctx.api.subscribers.messages.markAllAs({ messageId: message.id, markAs: "read" }, id);
  assert.equal((await next()).result.total, 0);

  await ctx.api.subscribers.messages.markAllAs({ messageId: message.id, markAs: "unread" }, id);
  assert.equal((await next()).result.total, 1);

  await ctx.api.messages.deleteByTransactionId(transactionId);
  assert.equal((await next()).result.total, 0);
});

test("realtime: a bad token cannot connect", async () => {
  const ws = new WebSocket(`${ctx.socketUrl}/?token=not-a-token`);
  const outcome = await within(
    new Promise<string>((resolve) => {
      ws.addEventListener("open", () => resolve("open"));
      ws.addEventListener("error", () => resolve("error"));
      ws.addEventListener("close", () => resolve("close"));
    }),
    3000,
    "socket outcome",
  );
  ws.close();
  assert.notEqual(outcome, "open");
});
