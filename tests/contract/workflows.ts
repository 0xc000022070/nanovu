import { SeverityLevelEnum, workflow } from "@novu/framework";
import { z } from "zod/v3";

type Payload = { [key: string]: unknown };

const text = (value: unknown) => (typeof value === "string" ? value : undefined);
const record = (value: unknown) =>
  typeof value === "object" && value !== null ? Object.fromEntries(Object.entries(value)) : {};

const message = (payload: Payload) => ({
  subject: text(payload.title),
  body: text(payload.body) ?? "",
  redirect: { url: "/r", target: "_self" as const },
  primaryAction: { label: "Open", redirect: { url: "/open" } },
  data: record(payload.data),
});

const open = { payloadSchema: { type: "object", additionalProperties: true } } as const;

const push = (payload: Payload) => ({ subject: text(payload.title) ?? "", body: text(payload.body) ?? "" });

export const alert = workflow(
  "alert",
  async ({ step, payload }) => {
    await step.inApp("in-app", async () => message(payload));
    await step.push("push", async () => push(payload));
  },
  { ...open, name: "Alert", tags: ["Activity"] },
);

export const digest = workflow(
  "digest",
  async ({ step }) => {
    const { events } = await step.digest("collect", async () => ({ amount: 2, unit: "seconds" }));
    await step.inApp("in-app", async () => ({ body: `${events.length} events` }));
    await step.push("push", async () => ({ subject: "Digest", body: `${events.length} events` }));
  },
  { tags: ["Activity"] },
);

export const delayed = workflow(
  "delayed",
  async ({ step, payload }) => {
    await step.delay("wait", async () => ({ type: "regular", amount: 1, unit: "seconds" }));
    await step.inApp("in-app", async () => message(payload), { skip: () => payload.skip === true });
    await step.push("push", async () => push(payload), { skip: () => payload.skip === true });
  },
  { ...open, tags: ["Activity"] },
);

export const email = workflow("email", async ({ step, payload }) => {
  await step.email("email", async () => ({ subject: text(payload.title) ?? "", body: `<p>${text(payload.body)}</p>` }));
}, open);

export const quiet = workflow(
  "quiet",
  async ({ step, payload }) => {
    if (payload.silent) return;
    await step.inApp("in-app", async () => message(payload));
  },
  { ...open, tags: ["Activity"] },
);

export const security = workflow(
  "security",
  async ({ step, payload }) => {
    await step.inApp("in-app", async () => message(payload));
    await step.push("push", async () => push(payload));
  },
  { ...open, tags: ["Security"], severity: SeverityLevelEnum.HIGH, preferences: { all: { readOnly: true } } },
);

export const strict = workflow(
  "strict",
  async ({ step, payload }) => {
    await step.inApp("in-app", async () => ({ body: `amount ${payload.amount}` }));
  },
  { payloadSchema: z.object({ amount: z.number() }) },
);

export const sender = workflow("sender", async ({ step, payload }) => {
  await step.email("email", async () => ({
    subject: text(payload.title) ?? "",
    body: `<p>${text(payload.body)}</p>`,
    from: record(payload.from),
    replyTo: "",
  }));
}, open);

export const tuned = workflow("tuned", async ({ step, payload }) => {
  await step.push("push", async () => push(payload), {
    providers: { fcm: () => ({ android: { priority: "high" } }) },
  });
}, open);

export const workflows = [alert, digest, delayed, email, quiet, security, strict, sender, tuned];
