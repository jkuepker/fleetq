import { getPreferenceValues } from "@raycast/api";
import { useFetch } from "@raycast/utils";
import { readFileSync } from "node:fs";
import { homedir, hostname, userInfo } from "node:os";
import { join } from "node:path";

// Subset of GET /v1/status the commands read.
export type Job = {
  id: number;
  state: string;
  kind: string;
  owner: string;
  label: string;
  hosts: string[];
  started_at: number | null;
  expect_s: number | null;
  start_in_s?: number | null;
  position?: number;
};

export type Service = { name: string; status: string; text: string; warn: boolean };

export type Host = {
  name: string;
  machine: string;
  online: boolean | null;
  capacity_gb: number;
  free_gb: number;
  services: Service[];
  running: Job[];
  queued: Job[];
  reservations: Job[];
};

export type Status = { now: number; last_probe: number | null; hosts: Host[]; warnings: string[] };

// Subset of GET /v1/models (one entry per URL and served name).
export type Model = {
  model: string;
  url: string;
  host: string;
  state: string;
  usable: boolean;
  capabilities: string[];
  context: number | null;
  root: string | null;
};

type Prefs = { serverUrl?: string; owners?: string };

// Older than this, the numbers on screen are not the fleet right now: a refresh failed, or the
// server stopped probing (it probes every 30 s).
export const STALE_S = 150;

function configFile(name: string): string | undefined {
  try {
    return readFileSync(join(homedir(), ".config", "fleetq", name), "utf8").trim() || undefined;
  } catch {
    return undefined;
  }
}

// Same lookup as the CLI (Raycast has no shell environment, so only the files), but with the
// read-only token: these commands never change the queue.
function server(): { url: string; token?: string } {
  const { serverUrl } = getPreferenceValues<Prefs>();
  const url = (serverUrl?.trim() || configFile("url") || "http://fleetq:8470").replace(/\/+$/, "");
  return { url, token: configFile("readonly_token") };
}

export const TOKEN_HELP = "Copy the server's /etc/fleetq/readonly_token to ~/.config/fleetq/readonly_token (chmod 600).";

function useFleetq<T>(path: string) {
  const { url, token } = server();
  const result = useFetch<T>(url + path, {
    headers: { Authorization: `Bearer ${token ?? ""}` },
    execute: !!token,
    keepPreviousData: true,
    parseResponse: async (res) => {
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(`${res.status} ${(body as { error?: string }).error ?? res.statusText}`);
      return body as T;
    },
    failureToastOptions: { title: "fleetq server unreachable" },
  });
  const error = token ? result.error : new Error(`No read-only token. ${TOKEN_HELP}`);
  return { ...result, error, url };
}

export const useFleetStatus = () => useFleetq<Status>("/v1/status");
export const useFleetModels = () => useFleetq<{ now: number; models: Model[] }>("/v1/models?all=1");

export function duration(seconds: number): string {
  const m = Math.max(0, Math.round(seconds / 60));
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h${String(m % 60).padStart(2, "0")}m`;
}

// A job on several hosts is listed under each of them; count it once.
function unique(jobs: Job[]): Job[] {
  return [...new Map(jobs.map((j) => [j.id, j])).values()];
}

export type Problem = { level: "red" | "orange"; text: string };

// What needs a look, worst first: an offline box or a service in trouble is red; a service held,
// restoring or starting on purpose, and the server's own warnings (overdue jobs, untracked GPU
// use) are orange. The server also warns about troubled services; those are listed once.
function attention(hosts: Host[], warnings: string[]): Problem[] {
  const out: Problem[] = [];
  const listed: string[] = [];
  for (const h of hosts) {
    if (h.online === false) out.push({ level: "red", text: `${h.name} is offline` });
    for (const s of h.services) {
      if (s.status === "up") continue;
      out.push({ level: s.warn ? "red" : "orange", text: `${s.name}: ${s.text}` });
      listed.push(`${s.name} on ${h.name}: `);
    }
  }
  for (const w of warnings) {
    if (!listed.some((prefix) => w.startsWith(prefix))) out.push({ level: "orange", text: w });
  }
  return out.sort((a, b) => (a.level === b.level ? 0 : a.level === "red" ? -1 : 1));
}

export function summarize(status: Status | undefined, error?: Error) {
  const hosts = status?.hosts ?? [];
  const running = unique(hosts.flatMap((h) => h.running));
  const queued = unique(hosts.flatMap((h) => h.queued));
  const booked = unique(hosts.flatMap((h) => h.reservations ?? [])).sort(
    (a, b) => (a.start_in_s ?? 0) - (b.start_in_s ?? 0),
  );
  const age = status ? Date.now() / 1000 - status.now : 0;
  const probeAge = status?.last_probe ? status.now - status.last_probe : 0;
  const stale = !!status && (!!error || age > STALE_S || probeAge > STALE_S);
  const staleText = !status
    ? ""
    : error || age > STALE_S
      ? `as of ${duration(age)} ago: ${error?.message ?? "no answer since"}`
      : `server last checked the fleet ${duration(probeAge)} ago`;
  return {
    hosts,
    running,
    queued,
    booked,
    attention: attention(hosts, status?.warnings ?? []),
    stale,
    staleText,
    idle: running.length === 0 && queued.length === 0,
  };
}

// Owners counted as "you": the preference (comma-separated, * matches anything), else the CLI's
// default owner, user@host.
export function isMine(job: Job): boolean {
  const { owners } = getPreferenceValues<Prefs>();
  const mine = owners?.trim()
    ? owners.split(",").map((o) => o.trim()).filter(Boolean)
    : [`${userInfo().username}@${hostname().split(".")[0]}`];
  return mine.some((pattern) => {
    const re = new RegExp("^" + pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*") + "$");
    return re.test(job.owner);
  });
}

export function jobTitle(job: Job): string {
  return `#${job.id} ${job.label}`;
}

// fleetq has no elapsed field: it is now - started_at, as `fleetq status` prints "for 22m".
export function elapsed(job: Job, now: number): string {
  if (!job.started_at) return "";
  const expect = job.expect_s ? ` of ~${duration(job.expect_s)}` : "";
  return `${duration(now - job.started_at)}${expect}`;
}

export function overdue(job: Job, now: number): boolean {
  return !!job.started_at && !!job.expect_s && now - job.started_at > job.expect_s;
}

export function jobLine(job: Job, now: number): string {
  const ran = elapsed(job, now);
  return `${jobTitle(job)}${ran ? ` · ${ran}` : ""}`;
}

export function bookedLine(job: Job): string {
  const when = job.start_in_s && job.start_in_s > 0 ? `in ${duration(job.start_in_s)}` : "now";
  const length = job.expect_s ? ` for ${duration(job.expect_s)}` : "";
  return `${job.hosts.join(" + ")} ${when}${length}`;
}

export function hostLine(host: Host, now: number): string {
  if (host.online === false) return "OFFLINE";
  const trouble = host.services.find((s) => s.status !== "up");
  const job = host.running[0];
  const jobs = job ? jobLine(job, now) + (host.running.length > 1 ? ` (+${host.running.length - 1})` : "") : "";
  if (trouble) return [trouble.text, jobs].filter(Boolean).join(" · ");
  if (jobs) return jobs;
  if (host.queued.length > 0) return "Nothing running, jobs waiting";
  return "Empty queue";
}

// The provider/model name `fleetq sync-agents` writes for Pi, omp and OpenCode: one provider per
// URL (fleetq-<host>-<port>), and the first served name for each root model on it.
export function harnessName(m: Model): string {
  const u = new URL(m.url);
  const port = u.port || (u.protocol === "https:" ? "443" : "80");
  return `fleetq-${m.host || u.hostname.split(".")[0]}-${port}/${m.model}`;
}

export function aliasOf(m: Model, all: Model[]): Model | undefined {
  if (!m.root) return undefined;
  const first = all.find((o) => o.url === m.url && o.root === m.root);
  return first && first.model !== m.model ? first : undefined;
}
