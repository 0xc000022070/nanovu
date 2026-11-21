import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { type Events, isSameFilter, type Novu as Inbox, type NotificationFilter, WorkflowCriticalityEnum } from "@novu/js";
import { sid, start, uid, unwrap, waitFor, type Ctx } from "../harness.ts";

let ctx: Ctx;
before(async () => {
  ctx = await start();
  await ctx.sync();
});
after(() => ctx?.stop());

type ListArgs = Parameters<Inbox["notifications"]["list"]>[0];

const list = async (novu: Inbox, args: ListArgs = {}) => unwrap(await novu.notifications.list({ limit: 50, ...args }));

async function seed(id: string, items: [string, { [key: string]: unknown }?][]) {
  for (const [workflowId, data = {}] of items) {
    await ctx.api.trigger({ workflowId, to: id, payload: { title: "Hello", body: uid("body"), data } });
  }
  const novu = ctx.inbox(id);
  await waitFor(async () => (await list(novu)).notifications.length === items.length, {
    what: `${items.length} inbox notifications`,
  });
  return novu;
}

const sessionResult = (novu: Inbox) =>
  new Promise<Events["session.initialize.resolved"]>((resolve) => {
    novu.on("session.initialize.resolved", resolve);
  });

test("inbox: session accepts a valid subscriber hash", async () => {
  const novu = ctx.inbox(sid());
  const session = sessionResult(novu);
  const page = await list(novu);
  assert.deepEqual(page.notifications, []);
  assert.equal(page.hasMore, false);
  const { data, error } = await session;
  assert.equal(error, undefined);
  assert.equal(typeof data?.token, "string");
  assert.equal(data?.unreadCount.total, 0);
});

test("inbox: session rejects a bad subscriber hash", async () => {
  const novu = ctx.inbox(sid(), { hash: "0".repeat(64) });
  const session = sessionResult(novu);
  const res = await novu.notifications.list();
  assert.ok(res.error, "list succeeded with a bad hash");
  const { error } = await session;
  assert.ok(error, "session initialized with a bad hash");
});

test("inbox: list filters by tags, read, archived and data", async () => {
  const novu = await seed(sid(), [
    ["alert", { kind: "a" }],
    ["alert", { kind: "b" }],
    ["security", { kind: "c" }],
  ]);
  const count = async (args: ListArgs) => (await list(novu, args)).notifications.length;
  assert.equal(await count({ tags: ["Activity"] }), 2);
  assert.equal(await count({ tags: ["Security"] }), 1);
  assert.equal(await count({ data: { kind: "a" } }), 1);

  const [first, second] = (await list(novu, { tags: ["Activity"] })).notifications;
  unwrap(await first.read());
  assert.equal(await count({ read: true }), 1);
  assert.equal(await count({ read: false }), 2);

  unwrap(await second.archive());
  assert.equal(await count({ archived: true }), 1);
  assert.equal(await count({ archived: false }), 2);
});

test("inbox: object tag filters on list, count and bulk", async () => {
  const novu = await seed(sid(), [["alert"], ["alert"], ["security"]]);
  const count = async (args: ListArgs) => (await list(novu, args)).notifications.length;
  const either = { or: ["Activity", "Security"] };
  const both = { and: [{ or: ["Activity", "Security"] }, { or: ["Activity"] }] };
  assert.equal(await count({ tags: either }), 3);
  assert.equal(await count({ tags: both }), 2);
  assert.equal(await count({ tags: { and: [{ or: ["Activity"] }, { or: ["Security"] }] } }), 0);

  const filters = [{ tags: either }, { tags: both, read: false }];
  const { counts } = unwrap(await novu.notifications.count({ filters }));
  const countOf = (filter: NotificationFilter) => counts.find((c) => isSameFilter(c.filter, filter))?.count;
  assert.deepEqual(filters.map(countOf), [3, 2]);

  unwrap(await novu.notifications.readAll({ tags: { and: [{ or: ["Security", "x"] }] } }));
  assert.equal(await count({ read: true, tags: ["Security"] }), 1);
  assert.equal(await count({ read: false }), 2);
});

test("inbox: data filters with nested paths, or, and and arrays", async () => {
  const novu = await seed(sid(), [
    ["alert", { kind: "a", meta: { team: "t1" }, labels: ["p", "q"] }],
    ["alert", { kind: "b", meta: { team: "t2" } }],
    ["alert", { kind: "c", flag: true }],
  ]);
  const kinds = async (data: ListArgs["data"]) =>
    (await list(novu, { data })).notifications.map((n) => n.data?.kind).sort();
  assert.deepEqual(await kinds({ meta: { team: "t2" } }), ["b"]);
  assert.deepEqual(await kinds({ kind: { or: ["a", "c"] } }), ["a", "c"]);
  assert.deepEqual(await kinds({ kind: { and: [{ or: ["a", "b"] }, { or: ["b", "c"] }] } }), ["b"]);
  assert.deepEqual(await kinds({ labels: "q" }), ["a"]);
  assert.deepEqual(await kinds({ flag: true, kind: ["c", "a"] }), ["c"]);

  const filters = [{ data: { meta: { team: { or: ["t1", "t2"] } } } }];
  const { counts } = unwrap(await novu.notifications.count({ filters }));
  assert.equal(counts.find((c) => isSameFilter(c.filter, filters[0]))?.count, 2);
});

test("inbox: list paginates newest first with after", async () => {
  const novu = await seed(sid(), [["alert"], ["alert"], ["alert"]]);
  const page1 = await list(novu, { limit: 2 });
  assert.equal(page1.notifications.length, 2);
  assert.equal(page1.hasMore, true);
  const page2 = await list(novu, { limit: 2, after: page1.notifications.at(-1)?.id });
  assert.equal(page2.notifications.length, 1);
  assert.equal(page2.hasMore, false);
  const all = [...page1.notifications, ...page2.notifications];
  assert.equal(new Set(all.map((n) => n.id)).size, 3);
  const times = all.map((n) => Date.parse(n.createdAt));
  assert.deepEqual(times, [...times].sort((a, b) => b - a));
});

test("inbox: counts several filters at once", async () => {
  const novu = await seed(sid(), [["alert"], ["alert"], ["security"]]);
  const [first] = (await list(novu, { tags: ["Activity"] })).notifications;
  unwrap(await first.read());

  const filters = [{ read: false }, { tags: ["Activity"], read: false }, { tags: ["Security"] }];
  const { counts } = unwrap(await novu.notifications.count({ filters }));
  const countOf = (filter: NotificationFilter) => counts.find((c) => isSameFilter(c.filter, filter))?.count;
  assert.deepEqual(filters.map(countOf), [2, 1, 1]);
});

test("inbox: single read, unread, archive and unarchive", async () => {
  const novu = await seed(sid(), [["alert"]]);
  const [n] = (await list(novu)).notifications;
  const read = unwrap(await n.read());
  assert.equal(read.isRead, true);
  assert.equal(typeof read.readAt, "string");
  assert.equal(unwrap(await n.unread()).isRead, false);
  const archived = unwrap(await n.archive());
  assert.equal(archived.isArchived, true);
  assert.equal(unwrap(await n.unarchive()).isArchived, false);
});

test("inbox: bulk read, seen and archive by tags", async () => {
  const novu = await seed(sid(), [["alert"], ["alert"], ["security"]]);
  const count = async (args: ListArgs) => (await list(novu, args)).notifications.length;

  unwrap(await novu.notifications.seenAll({ tags: ["Security"] }));
  assert.equal(await count({ tags: ["Security"], seen: false }), 0);
  assert.equal(await count({ tags: ["Activity"], seen: false }), 2);

  unwrap(await novu.notifications.readAll({ tags: ["Activity"] }));
  assert.equal(await count({ tags: ["Activity"], read: false }), 0);
  assert.equal(await count({ tags: ["Security"], read: false }), 1);

  unwrap(await novu.notifications.archiveAll({ tags: ["Activity"] }));
  assert.equal(await count({ archived: true }), 2);
  assert.equal(await count({ archived: false }), 1);
});

test("inbox: bulk read filtered by data", async () => {
  const novu = await seed(sid(), [
    ["alert", { kind: "a" }],
    ["alert", { kind: "b" }],
  ]);
  unwrap(await novu.notifications.readAll({ data: { kind: "a" } }));
  const unread = (await list(novu, { read: false })).notifications;
  assert.deepEqual(
    unread.map((n) => n.data),
    [{ kind: "b" }],
  );
});

test("inbox: preferences list and update, critical workflow hidden or refused", async () => {
  const novu = ctx.inbox(sid());
  const prefs = unwrap(await novu.preferences.list());
  assert.equal(prefs[0].level, "global");
  const alertPref = prefs.find((p) => p.workflow?.identifier === "alert");
  assert.ok(alertPref, "alert preference missing");
  assert.deepEqual(Object.keys(alertPref.channels).sort(), ["in_app", "push"]);
  assert.equal(alertPref.workflow?.critical, false);
  assert.ok(!prefs.some((p) => p.workflow?.identifier === "security"), "critical workflow listed as nonCritical");

  const updated = unwrap(await alertPref.update({ channels: { push: false } }));
  assert.equal(updated.channels.push, false);
  assert.equal(updated.channels.in_app, true);
  const again = unwrap(await novu.preferences.list()).find((p) => p.workflow?.identifier === "alert");
  assert.equal(again?.channels.push, false);

  const global = unwrap(await novu.preferences.list()).find((p) => p.level === "global");
  assert.ok(global, "global preference missing");
  unwrap(await global.update({ channels: { push: true } }));
  const reset = unwrap(await novu.preferences.list()).find((p) => p.workflow?.identifier === "alert");
  assert.equal(reset?.channels.push, true);

  const all = unwrap(await novu.preferences.list({ criticality: WorkflowCriticalityEnum.ALL }));
  const critical = all.find((p) => p.workflow?.identifier === "security");
  if (critical) {
    assert.equal(critical.workflow?.critical, true);
    const res = await critical.update({ channels: { push: false } });
    assert.ok(res.error, "critical workflow preference accepted an update");
  }
});
