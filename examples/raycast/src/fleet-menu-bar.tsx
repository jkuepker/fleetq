import { Clipboard, Color, Icon, launchCommand, LaunchType, MenuBarExtra } from "@raycast/api";
import {
  bookedLine,
  elapsed,
  hostLine,
  isMine,
  Job,
  jobTitle,
  overdue,
  summarize,
  useFleetStatus,
} from "./fleet";

// Menu titles are plain strings (no font or color), so bold comes from Unicode
// Mathematical Sans-Serif Bold letters and digits; status color goes on the icon.
function bold(text: string): string {
  return [...text.toUpperCase()]
    .map((c) => {
      if (c >= "A" && c <= "Z") return String.fromCodePoint(0x1d5d4 + c.charCodeAt(0) - 65);
      if (c >= "0" && c <= "9") return String.fromCodePoint(0x1d7ec + c.charCodeAt(0) - 48);
      return c;
    })
    .join("");
}

function JobItem({ job, now }: { job: Job; now: number }) {
  const tint = job.state !== "running" ? Color.Blue : overdue(job, now) ? Color.Red : Color.Yellow;
  const mine = isMine(job);
  return (
    <MenuBarExtra.Item
      icon={{ source: mine ? Icon.Person : Icon.Tag, tintColor: tint }}
      title={`${job.hosts.map(bold).join(" + ")}   ${jobTitle(job)}`}
      subtitle={[mine ? "yours" : "", elapsed(job, now)].filter(Boolean).join(" · ") || undefined}
      tooltip={`${job.owner}. Click to copy the job ID`}
      onAction={() => Clipboard.copy(String(job.id))}
    />
  );
}

function open(name: string) {
  return () => launchCommand({ name, type: LaunchType.UserInitiated });
}

export default function Command() {
  const { data, isLoading, error, revalidate } = useFleetStatus();
  const now = data?.now ?? Date.now() / 1000;
  const { hosts, running, queued, booked, attention, stale, staleText, idle } = summarize(data, error);
  const red = attention.some((p) => p.level === "red");

  let title: string | undefined;
  let tint: Color;
  if (error && !data) {
    title = "fleetq ?";
    tint = Color.Red;
  } else if (!data) {
    title = undefined;
    tint = Color.SecondaryText;
  } else {
    title = idle ? "empty queue" : `${running.length} running · ${queued.length} queued`;
    if (attention.length) title += ` · ⚠ ${attention.length}`;
    if (stale) title += " · stale";
    // Grey when the numbers are old; otherwise the worst thing on the fleet sets the color.
    tint = stale
      ? Color.SecondaryText
      : red
        ? Color.Red
        : attention.length
          ? Color.Orange
          : running.length > 0
            ? Color.Yellow
            : queued.length > 0
              ? Color.Orange
              : Color.Green;
  }

  return (
    <MenuBarExtra icon={{ source: Icon.HardDrive, tintColor: tint }} title={title} tooltip="fleetq" isLoading={isLoading}>
      {error && !data && <MenuBarExtra.Item title={`fleetq failed: ${error.message}`} />}
      {stale && <MenuBarExtra.Item icon={{ source: Icon.Clock, tintColor: Color.SecondaryText }} title={staleText} />}
      {attention.length > 0 && (
        <MenuBarExtra.Section title="Attention">
          {attention.map((p) => (
            <MenuBarExtra.Item
              key={p.text}
              icon={{ source: Icon.Warning, tintColor: p.level === "red" ? Color.Red : Color.Orange }}
              title={p.text}
              tooltip="Click to copy"
              onAction={() => Clipboard.copy(p.text)}
            />
          ))}
        </MenuBarExtra.Section>
      )}
      {running.length > 0 && (
        <MenuBarExtra.Section title="Running">
          {running.map((job) => (
            <JobItem key={job.id} job={job} now={now} />
          ))}
        </MenuBarExtra.Section>
      )}
      {queued.length > 0 && (
        <MenuBarExtra.Section title={`Queued (${queued.length})`}>
          {queued.map((job) => (
            <JobItem key={job.id} job={job} now={now} />
          ))}
        </MenuBarExtra.Section>
      )}
      {booked.length > 0 && (
        <MenuBarExtra.Section title="Booked">
          {booked.map((job) => (
            <MenuBarExtra.Item
              key={job.id}
              icon={{ source: isMine(job) ? Icon.Person : Icon.Calendar, tintColor: Color.Purple }}
              title={`${bookedLine(job)}   ${jobTitle(job)}`}
              tooltip={`${job.owner}. Click to copy the job ID`}
              onAction={() => Clipboard.copy(String(job.id))}
            />
          ))}
        </MenuBarExtra.Section>
      )}
      {data && (
        <MenuBarExtra.Section title="Fleet">
          {hosts.map((host) => {
            const bad = host.online === false || host.services.some((s) => s.warn);
            const odd = host.services.some((s) => s.status !== "up");
            return (
              <MenuBarExtra.Item
                key={host.name}
                icon={{
                  source: Icon.CircleFilled,
                  tintColor: bad ? Color.Red : odd ? Color.Orange : host.running.length ? Color.Yellow : Color.Green,
                }}
                title={bold(host.name)}
                subtitle={hostLine(host, now)}
              />
            );
          })}
        </MenuBarExtra.Section>
      )}
      <MenuBarExtra.Section>
        <MenuBarExtra.Item
          title="Open Fleet Status"
          icon={Icon.List}
          shortcut={{ modifiers: ["cmd"], key: "o" }}
          onAction={open("fleet-status")}
        />
        <MenuBarExtra.Item
          title="Open Fleet Models"
          icon={Icon.Stars}
          shortcut={{ modifiers: ["cmd"], key: "m" }}
          onAction={open("fleet-models")}
        />
        <MenuBarExtra.Item
          title="Refresh"
          icon={Icon.ArrowClockwise}
          shortcut={{ modifiers: ["cmd"], key: "r" }}
          onAction={revalidate}
        />
      </MenuBarExtra.Section>
    </MenuBarExtra>
  );
}
