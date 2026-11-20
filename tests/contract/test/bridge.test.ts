import assert from "node:assert/strict";
import { once } from "node:events";
import { connect } from "node:net";
import { after, before, test } from "node:test";
import { at, signJwt, signature, start, type Ctx } from "../harness.ts";
import { workflows } from "../workflows.ts";

let ctx: Ctx;
before(async () => {
  ctx = await start();
});
after(() => ctx?.stop());

test("bridge: signed discover lists every workflow", async () => {
  const res = await fetch(`${ctx.bridgeUrl()}?action=discover`, {
    headers: { "novu-signature": signature(ctx.secretKey, {}) },
  });
  assert.equal(res.status, 200);
  const listed = at(await res.json(), "workflows");
  assert.ok(Array.isArray(listed));
  assert.deepEqual(
    listed.map((w: unknown) => at(w, "workflowId")),
    workflows.map((w) => w.id),
  );
});

test("bridge: unsigned or wrongly signed requests are rejected", async () => {
  const discover = await fetch(`${ctx.bridgeUrl()}?action=discover`);
  assert.equal(discover.status, 401, await discover.text());
  const body = { payload: {}, subscriber: {}, state: [], controls: {}, context: {}, env: {} };
  const execute = await fetch(`${ctx.bridgeUrl()}?action=execute&workflowId=alert&stepId=in-app`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  assert.equal(execute.status, 401, await execute.text());
  const forged = await fetch(`${ctx.bridgeUrl()}?action=execute&workflowId=alert&stepId=in-app`, {
    method: "POST",
    headers: { "content-type": "application/json", "novu-signature": signature("x".repeat(40), body) },
    body: JSON.stringify(body),
  });
  assert.equal(forged.status, 401, await forged.text());
});

test("harness: fake Google token and FCM endpoints record and answer UNREGISTERED", async () => {
  const sa = ctx.serviceAccount;
  const assertion = signJwt(sa.private_key, { iss: sa.client_email, aud: sa.token_uri });
  const token = await fetch(sa.token_uri, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion }),
  });
  assert.equal(at(await token.json(), "access_token"), "fake-access-token");
  assert.equal(ctx.google.tokens.at(-1)?.valid, true);

  const fcm = new URL("/v1/projects/contract/messages:send", sa.token_uri).href;
  const send = (tok: string) =>
    fetch(fcm, { method: "POST", body: JSON.stringify({ message: { token: tok, notification: { title: "t" } } }) });
  assert.equal((await send("good")).status, 200);
  ctx.google.unregistered.add("gone");
  const gone = await send("gone");
  assert.equal(gone.status, 404);
  assert.equal(at(await gone.json(), "error", "details", "0", "errorCode"), "UNREGISTERED");
  assert.deepEqual(
    ctx.google.sends.slice(-2).map((r) => r.message.token),
    ["good", "gone"],
  );
});

test("harness: SMTP sink records a mail", async () => {
  const port = Number(at(ctx.smtp.server.address(), "port"));
  const sock = connect(port, "127.0.0.1");
  let got = "";
  sock.on("data", (c) => {
    got += c;
  });
  const lines = ["EHLO t", "MAIL FROM:<a@x.test>", "RCPT TO:<b@x.test>", "DATA", "Subject: hi\r\n\r\nbody\r\n.", "QUIT"];
  for (const line of lines) sock.write(`${line}\r\n`);
  await once(sock, "close");
  assert.match(got, /^220 /);
  const mail = ctx.smtp.mails.at(-1);
  assert.ok(mail, "no mail recorded");
  assert.deepEqual(mail.to, ["b@x.test"]);
  assert.match(mail.data, /Subject: hi/);
});
