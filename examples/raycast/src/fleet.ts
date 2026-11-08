import { getPreferenceValues } from "@raycast/api";
import { useExec } from "@raycast/utils";
import { homedir } from "node:os";

// Subset of `fleetq status --json` (GET /v1/status) the commands read.
export type Job = {
  id: number;
  state: string;
  owner: string;
  label: string;
  hosts: string[];
  started_at: number | null;
  expect_s: number | null;
  position?: number;
};

export type Host = {
  name: string;
  machine: string;
  online: boolean | null;
  capacity_gb: number;
  free_gb: number;
  services: { name: string; status: string }[];
  running: Job[];
  queued: Job[];
};

export type Status = { now: number; hosts: Host[] };

function fleetqPath(): string {
  const { fleetqPath } = getPreferenceValues<{ fleetqPath?: string }>();
  const path = fleetqPath?.trim() || "~/.local/bin/fleetq";
  return path.startsWith("~/") ? homedir() + path.slice(1) : path;
}

export function useFleetStatus() {
  return useExec(fleetqPath(), ["status", "--json"], {
    parseOutput: ({ stdout }) => JSON.parse(stdout) as Status,
    keepPreviousData: true,
    failureToastOptions: { title: "fleetq status failed" },
  });
}

export function duration(seconds: number): string {
  const m = Math.max(0, Math.round(seconds / 60));
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h${String(m % 60).padStart(2, "0")}m`;
}

// A job on several hosts is listed under each of them; count it once.
function unique(jobs: Job[]): Job[] {
  return [...new Map(jobs.map((j) => [j.id, j])).values()];
}

export function summarize(status: Status | undefined) {
  const hosts = status?.hosts ?? [];
  const running = unique(hosts.flatMap((h) => h.running));
  const queued = unique(hosts.flatMap((h) => h.queued));
  return { hosts, running, queued, idle: running.length === 0 && queued.length === 0 };
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

export function hostLine(host: Host, now: number): string {
  const job = host.running[0];
  if (job) return jobLine(job, now);
  if (host.queued.length > 0) return "Nothing running, jobs waiting";
  return "Empty queue";
}
