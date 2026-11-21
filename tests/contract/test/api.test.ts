import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { ErrorDto, PayloadValidationExceptionDto, ValidationErrorDto } from "@novu/api/models/errors/index.js";
import { at, BRIDGE_LESS_PATH, envId, expectError, sid, start, uid, type Ctx } from "../harness.ts";
import { alert, workflows } from "../workflows.ts";

let ctx: Ctx;
before(async () => {
  ctx = await start();
  await ctx.sync();
});
after(() => ctx?.stop());

const payload = () => ({ title: "Hello", body: uid("body") });

async function deliver(subscriberId: string, workflowId = "alert") {
  const transactionId = uid("tx");
  await ctx.api.trigger({ workflowId, to: subscriberId, payload: payload(), transactionId });
  return { transactionId, message: await ctx.waitForInApp(subscriberId, transactionId) };
}

test("api: bridge sync replaces the workflow set", async () => {
  try {
    const full = await ctx.sync();
    assert.ok(full.status >= 200 && full.status < 300, JSON.stringify(full));
    assert.equal(at(full.body, "data", "length"), workflows.length);

    const less = await ctx.sync(BRIDGE_LESS_PATH);
    assert.ok(less.status >= 200 && less.status < 300, JSON.stringify(less));
    assert.equal(at(less.body, "data", "length"), workflows.length - 1);
    await expectError(
      ctx.api.trigger({ workflowId: "email", to: sid(), payload: payload() }),
      ValidationErrorDto,
      422,
    );
  } finally {
    await ctx.sync();
  }
});

test("api: subscriber create, patch and delete", async () => {
  const id = sid();
  const { result: created } = await ctx.api.subscribers.create({ subscriberId: id, firstName: "Ada" });
  assert.equal(created.subscriberId, id);
  assert.equal(created.firstName, "Ada");
  assert.equal(created.deleted, false);

  const { result: patched } = await ctx.api.subscribers.patch({ lastName: "Lovelace" }, id);
  assert.equal(patched.firstName, "Ada");
  assert.equal(patched.lastName, "Lovelace");

  await ctx.api.subscribers.patch({ email: `${id}@contract.test` }, id);
  const { result: cleared } = await ctx.api.subscribers.patch({ email: null }, id);
  assert.equal(cleared.email, undefined);
  assert.equal(cleared.lastName, "Lovelace");
  const { result: read } = await ctx.api.subscribers.patch({ firstName: "Ada" }, id);
  assert.equal(read.email, undefined);

  const { result: deleted } = await ctx.api.subscribers.delete(id);
  assert.deepEqual(deleted, { acknowledged: true, status: "deleted" });
});

test("api: missing subscriber answers 404", async () => {
  await expectError(ctx.api.subscribers.patch({ firstName: "x" }, sid()), ErrorDto, 404);
  await expectError(ctx.api.subscribers.delete(sid()), ErrorDto, 404);
});

test("api: credentials replace device tokens", async () => {
  const id = sid();
  await ctx.api.subscribers.create({ subscriberId: id });
  await ctx.api.subscribers.credentials.update(
    { providerId: "fcm", credentials: { deviceTokens: [uid("tok")] } },
    id,
  );
});

test("api: credentials without an fcm integration answer the Novu message", async () => {
  const id = sid();
  await ctx.bareApi.subscribers.create({ subscriberId: id });
  const err = await expectError(
    ctx.bareApi.subscribers.credentials.update({ providerId: "fcm", credentials: { deviceTokens: ["t"] } }, id),
    ErrorDto,
    400,
  );
  assert.match(err.message, /^Subscribers environment \([a-f0-9]+\) do not have active fcm integration\.$/);
  assert.ok(err.message.includes(envId("bare")));
});

test("api: topics and subscriptions add and remove", async () => {
  const key = uid("topic");
  const [a, b] = [sid(), sid()];
  for (const subscriberId of [a, b]) await ctx.api.subscribers.create({ subscriberId });
  const { result: topic } = await ctx.api.topics.create({ key, name: "Contract" });
  assert.equal(topic.key, key);

  const { result: added } = await ctx.api.topics.subscriptions.create(
    { subscriptions: [{ identifier: `${key}:${a}`, subscriberId: a }, { identifier: `${key}:${b}`, subscriberId: b }] },
    key,
  );
  assert.equal(added.data.length, 2);
  assert.equal(added.meta.successful, 2);

  const { result: removed } = await ctx.api.topics.subscriptions.delete(
    { subscriptions: [{ identifier: `${key}:${a}`, subscriberId: a }] },
    key,
  );
  assert.equal(removed.data.length, 1);
});

test("api: topic subscriptions where every entry fails answer 400", async () => {
  const key = uid("topic");
  await ctx.api.topics.create({ key, name: "Contract" });
  const missing = sid();
  const subscriptions = [{ identifier: `${key}:${missing}`, subscriberId: missing }];
  for (const call of [ctx.api.topics.subscriptions.create, ctx.api.topics.subscriptions.delete]) {
    const err = await expectError(call.call(ctx.api.topics.subscriptions, { subscriptions }, key), ErrorDto, 400);
    assert.deepEqual(at(err.ctx, "meta"), { totalCount: 1, successful: 0, failed: 1 });
    assert.equal(at(err.ctx, "errors", "0", "subscriberId"), missing);
  }
});

test("api: framework workflow.trigger reaches one subscriber", async () => {
  const id = sid();
  const transactionId = uid("tx");
  const res = await alert.trigger({ to: { subscriberId: id }, payload: payload(), transactionId });
  assert.equal(at(res, "data", "data", "transactionId"), transactionId);
  await ctx.waitForInApp(id, transactionId);
});

test("api: trigger to an array of recipients", async () => {
  const [a, b] = [sid(), sid()];
  const transactionId = uid("tx");
  const { result } = await ctx.api.trigger({
    workflowId: "alert",
    to: [a, { subscriberId: b, email: `${b}@contract.test` }],
    payload: payload(),
    transactionId,
  });
  assert.equal(result.acknowledged, true);
  assert.equal(result.transactionId, transactionId);
  await Promise.all([ctx.waitForInApp(a, transactionId), ctx.waitForInApp(b, transactionId)]);
});

test("api: trigger to a topic excludes the actor", async () => {
  const key = uid("topic");
  const [actor, b, c] = [sid(), sid(), sid()];
  for (const subscriberId of [actor, b, c]) await ctx.api.subscribers.create({ subscriberId });
  await ctx.api.topics.subscriptions.create(
    { subscriptions: [actor, b, c].map((subscriberId) => ({ identifier: `${key}:${subscriberId}`, subscriberId })) },
    key,
  );
  const transactionId = uid("tx");
  await ctx.api.trigger({
    workflowId: "alert",
    to: { type: "Topic", topicKey: key },
    actor: { subscriberId: actor },
    payload: payload(),
    transactionId,
  });
  await Promise.all([ctx.waitForInApp(b, transactionId), ctx.waitForInApp(c, transactionId)]);
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(actor, transactionId), []);
});

test("api: trigger to a topic honours exclude unless the subscriber is addressed directly", async () => {
  const key = uid("topic");
  const [a, b, c] = [sid(), sid(), sid()];
  for (const subscriberId of [a, b, c]) await ctx.api.subscribers.create({ subscriberId });
  await ctx.api.topics.subscriptions.create(
    { subscriptions: [a, b, c].map((subscriberId) => ({ identifier: `${key}:${subscriberId}`, subscriberId })) },
    key,
  );
  const excluded = uid("tx");
  await ctx.api.trigger({ workflowId: "alert", to: { type: "Topic", topicKey: key, exclude: [b] }, payload: payload(), transactionId: excluded });
  const direct = uid("tx");
  await ctx.api.trigger({
    workflowId: "alert",
    to: [{ type: "Topic", topicKey: key, exclude: [c] }, c],
    payload: payload(),
    transactionId: direct,
  });
  await Promise.all([ctx.waitForInApp(a, excluded), ctx.waitForInApp(c, excluded), ctx.waitForInApp(c, direct)]);
  await ctx.settle();
  assert.deepEqual(await ctx.inAppMessages(b, excluded), []);
});

test("api: unknown workflow answers 422", async () => {
  await expectError(ctx.api.trigger({ workflowId: "nope", to: sid(), payload: {} }), ValidationErrorDto, 422);
});

test("api: payload failing the workflow schema answers 400", async () => {
  await expectError(
    ctx.api.trigger({ workflowId: "strict", to: sid(), payload: { amount: "x" } }),
    PayloadValidationExceptionDto,
    400,
  );
});

test("api: a workflow without a payload schema accepts any payload", async () => {
  const id = sid();
  const transactionId = uid("tx");
  const { result } = await ctx.api.trigger({ workflowId: "digest", to: id, payload: payload(), transactionId });
  assert.equal(result.acknowledged, true);
  await ctx.waitForInApp(id, transactionId);
});

test("api: idempotency-key replays the first trigger", async () => {
  const id = sid();
  const key = randomUUID();
  const dto = { workflowId: "alert", to: id, payload: payload() };
  const { result: first } = await ctx.api.trigger(dto, key);
  const { result: second } = await ctx.api.trigger(dto, key);
  assert.equal(second.transactionId, first.transactionId);
  await ctx.waitForInApp(id, first.transactionId);
  await ctx.settle();
  assert.equal((await ctx.inAppMessages(id)).length, 1);
});

test("api: a reused transactionId is acknowledged and dropped until its messages are deleted", async () => {
  const id = sid();
  const { transactionId } = await deliver(id);
  const { result } = await ctx.api.trigger({ workflowId: "alert", to: id, payload: payload(), transactionId });
  assert.equal(result.acknowledged, true);
  assert.equal(result.transactionId, transactionId);
  await ctx.settle();
  assert.equal((await ctx.inAppMessages(id, transactionId)).length, 1);

  await ctx.api.messages.deleteByTransactionId(transactionId);
  await ctx.api.trigger({ workflowId: "alert", to: id, payload: payload(), transactionId });
  await ctx.waitForInApp(id, transactionId);
});

test("api: framework workflow.trigger with a reused transactionId delivers once", async () => {
  const id = sid();
  const transactionId = uid("tx");
  for (let i = 0; i < 2; i++) {
    const res = await alert.trigger({ to: { subscriberId: id }, payload: payload(), transactionId });
    assert.equal(at(res, "data", "data", "transactionId"), transactionId);
  }
  await ctx.waitForInApp(id, transactionId);
  await ctx.settle();
  assert.equal((await ctx.inAppMessages(id, transactionId)).length, 1);
});

test("api: cancel answers true for pending runs and false otherwise", async () => {
  const transactionId = uid("tx");
  await ctx.api.trigger({ workflowId: "delayed", to: sid(), payload: payload(), transactionId });
  assert.equal((await ctx.api.cancel(transactionId)).result, true);
  assert.equal((await ctx.api.cancel(transactionId)).result, false);
  assert.equal((await ctx.api.cancel(uid("tx"))).result, false);
});

test("api: mark-as updates the message", async () => {
  const id = sid();
  const { message } = await deliver(id);
  assert.ok(message.id);
  const { result: read } = await ctx.api.subscribers.messages.markAllAs({ messageId: message.id, markAs: "read" }, id);
  assert.equal(read.length, 1);
  assert.equal(read[0].read, true);
  const { result: unread } = await ctx.api.subscribers.messages.markAllAs(
    { messageId: [message.id], markAs: "unread" },
    id,
  );
  assert.equal(unread[0].read, false);
});

test("api: unknown subscriber answers 404 on mark-as and messages list", async () => {
  const id = sid();
  const { message } = await deliver(id);
  const other = sid();
  await expectError(
    ctx.api.subscribers.messages.markAllAs({ messageId: message.id, markAs: "read" }, other),
    ErrorDto,
    404,
  );
  await expectError(ctx.api.messages.retrieve({ subscriberId: other }), ErrorDto, 404);
});

test("api: messages list filters by transactionId", async () => {
  const id = sid();
  const one = await deliver(id);
  const two = await deliver(id);
  const listed = await ctx.inAppMessages(id, one.transactionId);
  assert.equal(listed.length, 1);
  assert.equal(listed[0].transactionId, one.transactionId);
  assert.equal((await ctx.inAppMessages(id)).length, 2);
  assert.notEqual(one.message.id, two.message.id);
});

test("api: delete by transactionId removes the messages", async () => {
  const id = sid();
  const { transactionId } = await deliver(id);
  await ctx.api.messages.deleteByTransactionId(transactionId);
  assert.deepEqual(await ctx.inAppMessages(id, transactionId), []);
});

test("api: delete by transactionId honours the channel", async () => {
  const id = sid();
  const { transactionId } = await deliver(id);
  await ctx.api.messages.deleteByTransactionId(transactionId, "push");
  assert.equal((await ctx.inAppMessages(id, transactionId)).length, 1);
  await ctx.api.messages.deleteByTransactionId(transactionId, "in_app");
  assert.deepEqual(await ctx.inAppMessages(id, transactionId), []);
  await ctx.api.messages.deleteByTransactionId(uid("tx"), "in_app");
});
