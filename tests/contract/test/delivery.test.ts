import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { sid, start, uid, unwrap, waitFor, type Ctx } from "../harness.ts";

let ctx: Ctx;
before(async () => {
  ctx = await start();
  await ctx.sync();
});
after(() => ctx?.stop());

async function withToken(subscriberId: string) {
  const token = uid("tok");
  await ctx.api.subscribers.create({ subscriberId });
  await ctx.api.subscribers.credentials.update({ providerId: "fcm", credentials: { deviceTokens: [token] } }, subscriberId);
  return token;
}

async function trigger(workflowId: string, to: Parameters<Ctx["api"]["trigger"]>[0]["to"], extra = {}) {
  const transactionId = uid("tx");
  const body = uid("body");
  await ctx.api.trigger({ workflowId, to, payload: { title: "Hello", body, ...extra }, transactionId });
  return { transactionId, body };
}

const waitForPush = (body: string, timeout?: number) =>
  waitFor(() => ctx.pushesWithBody(body)[0], { timeout, what: `FCM send with body ${body}` });

test("delivery: in-app message is visible through @novu/js", async () => {
  const id = sid();
  const { transactionId, body } = await trigger("alert", id, { data: { kind: "a" } });
  const novu = ctx.inbox(id);
  const n = await waitFor(
    async () => unwrap(await novu.notifications.list()).notifications.find((x) => x.transactionId === transactionId),
    { what: "inbox notification" },
  );
  assert.equal(n.subject, "Hello");
  assert.equal(n.body, body);
  assert.equal(n.isRead, false);
  assert.equal(n.isSeen, false);
  assert.equal(n.isArchived, false);
  assert.equal(n.channelType, "in_app");
  assert.deepEqual(n.tags, ["Activity"]);
  assert.deepEqual(n.data, { kind: "a" });
  assert.equal(n.primaryAction?.label, "Open");
  assert.equal(n.redirect?.url, "/r");
  assert.equal(n.to.subscriberId, id);
  assert.ok(!Number.isNaN(Date.parse(n.createdAt)));
});

test("delivery: push reaches FCM with token, title and body", async () => {
  const id = sid();
  const token = await withToken(id);
  const { body } = await trigger("alert", id);
  const sent = await waitForPush(body);
  assert.equal(sent.message.token, token);
  assert.equal(sent.message.notification.title, "Hello");
  assert.equal(sent.authorization, "Bearer fake-access-token");
  assert.match(sent.path, /\/projects\/contract\/messages:send$/);
  for (const value of Object.values(sent.message.data ?? {})) assert.equal(typeof value, "string");
  const grant = ctx.google.tokens.at(-1);
  assert.ok(grant, "no token grant recorded");
  assert.equal(grant.valid, true);
  assert.equal(grant.form.grant_type, "urn:ietf:params:oauth:grant-type:jwt-bearer");
  assert.equal(grant.claims.iss, ctx.serviceAccount.client_email);
  assert.match(String(grant.claims.scope), /firebase\.messaging|cloud-platform/);
});

test("delivery: UNREGISTERED token is removed", async () => {
  const id = sid();
  const [good, gone] = [uid("tok"), uid("tok")];
  ctx.google.unregistered.add(gone);
  await ctx.api.subscribers.create({ subscriberId: id });
  await ctx.api.subscribers.credentials.update({ providerId: "fcm", credentials: { deviceTokens: [good, gone] } }, id);

  const first = await trigger("alert", id);
  await waitFor(() => ctx.pushesWithBody(first.body).length === 2, { what: "both tokens tried" });
  const second = await trigger("alert", id);
  await waitForPush(second.body);
  await ctx.settle();
  assert.deepEqual(
    ctx.pushesWithBody(second.body).map((r) => r.message.token),
    [good],
  );
});

test("delivery: digest merges triggers into one message", async () => {
  const id = sid();
  const token = await withToken(id);
  await Promise.all([1, 2, 3].map((n) => trigger("digest", id, { n })));
  const message = await ctx.waitForInApp(id, undefined, 8000);
  await ctx.settle();
  const all = await ctx.inAppMessages(id);
  assert.equal(all.length, 1);
  assert.equal(message.content, "3 events");
  assert.equal(ctx.pushesWithBody("3 events").filter((r) => r.message.token === token).length, 1);
});

test("delivery: delay delivers after the window and skip suppresses", async () => {
  const id = sid();
  const token = await withToken(id);
  const started = Date.now();
  const kept = await trigger("delayed", id);
  const skipped = await trigger("delayed", id, { skip: true });
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(id), []);
  await ctx.waitForInApp(id, kept.transactionId, 6000);
  assert.ok(Date.now() - started >= 1000, "delivered before the delay window");
  await waitForPush(kept.body);
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(id, skipped.transactionId), []);
  assert.deepEqual(ctx.pushesWithBody(skipped.body), []);
  assert.equal(ctx.pushesWithBody(kept.body)[0].message.token, token);
});

test("delivery: cancel before the delay prevents delivery", async () => {
  const id = sid();
  const cancelled = await trigger("delayed", id);
  assert.equal((await ctx.api.cancel(cancelled.transactionId)).result, true);
  const kept = await trigger("delayed", id);
  await ctx.waitForInApp(id, kept.transactionId, 6000);
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(id, cancelled.transactionId), []);
});

test("delivery: email reaches the SMTP sink", async () => {
  const id = sid();
  const address = `${id}@contract.test`;
  const { body } = await trigger("email", { subscriberId: id, email: address });
  const mail = await waitFor(() => ctx.smtp.mails.find((m) => m.to.includes(address)), { what: "SMTP mail" });
  assert.equal(mail.from, "nanovu@contract.test");
  assert.match(mail.data, /^Subject: Hello\r?$/im);
  assert.ok(mail.data.includes(body), "mail body carries the step body");
});

test("delivery: early return produces nothing", async () => {
  const id = sid();
  const silent = await trigger("quiet", id, { silent: true });
  const loud = await trigger("quiet", id);
  await ctx.waitForInApp(id, loud.transactionId);
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(id, silent.transactionId), []);
});

test("delivery: push preference off suppresses push but not in-app", async () => {
  const id = sid();
  await withToken(id);
  const novu = ctx.inbox(id);
  const prefs = unwrap(await novu.preferences.list());
  const pref = prefs.find((p) => p.workflow?.identifier === "alert");
  assert.ok(pref, `alert preference missing in ${JSON.stringify(prefs.map((p) => p.workflow?.identifier ?? p.level))}`);
  assert.equal(unwrap(await pref.update({ channels: { push: false } })).channels.push, false);

  const { transactionId, body } = await trigger("alert", id);
  await ctx.waitForInApp(id, transactionId);
  await ctx.settle();
  assert.deepEqual(ctx.pushesWithBody(body), []);
});

test("delivery: critical workflow ignores preferences", async () => {
  const id = sid();
  await withToken(id);
  const novu = ctx.inbox(id);
  const global = unwrap(await novu.preferences.list()).find((p) => p.level === "global");
  assert.ok(global, "global preference missing");
  assert.equal(unwrap(await global.update({ channels: { push: false, in_app: false } })).channels.push, false);

  const { transactionId, body } = await trigger("security", id);
  await ctx.waitForInApp(id, transactionId);
  await waitForPush(body);
});
