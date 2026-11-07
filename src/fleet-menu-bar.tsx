import { Clipboard, Color, Icon, launchCommand, LaunchType, MenuBarExtra } from "@raycast/api";
import { elapsed, hostLine, Job, jobTitle, overdue, summarize, useFleetStatus } from "./fleet";

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
  return (
    <MenuBarExtra.Item
      icon={{ source: Icon.Tag, tintColor: tint }}
      title={`${job.hosts.map(bold).join(" + ")}   ${jobTitle(job)}`}
      subtitle={elapsed(job, now) || undefined}
      tooltip="Copy job ID"
      onAction={() => Clipboard.copy(String(job.id))}
    />
  );
}

export default function Command() {
  const { data, isLoading, error, revalidate } = useFleetStatus();
  const now = data?.now ?? Date.now() / 1000;
  const { hosts, running, queued, idle } = summarize(data);

  let title: string | undefined;
  let tint: Color;
  if (error && !data) {
    title = "fleetq ?";
    tint = Color.Red;
  } else if (!data) {
    title = undefined;
    tint = Color.SecondaryText;
  } else if (idle) {
    title = "empty queue";
    tint = Color.Green;
  } else {
    title = `${running.length} running · ${queued.length} queued`;
    tint = running.length > 0 ? Color.Yellow : Color.Orange;
  }

  return (
    <MenuBarExtra
      icon={{ source: Icon.HardDrive, tintColor: tint }}
      title={title}
      tooltip="fleetq"
      isLoading={isLoading}
    >
      {error && !data && <MenuBarExtra.Item title={`fleetq failed: ${error.message}`} />}
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
      {data && (
        <MenuBarExtra.Section title="Fleet">
          {hosts.map((host) => (
            <MenuBarExtra.Item
              key={host.name}
              icon={{
                source: Icon.CircleFilled,
                tintColor: host.online === false ? Color.Red : host.running.length ? Color.Yellow : Color.Green,
              }}
              title={bold(host.name)}
              subtitle={host.online === false ? "OFFLINE" : hostLine(host, now)}
            />
          ))}
        </MenuBarExtra.Section>
      )}
      <MenuBarExtra.Section>
        <MenuBarExtra.Item
          title="Open Fleet Status"
          icon={Icon.List}
          shortcut={{ modifiers: ["cmd"], key: "o" }}
          onAction={() => launchCommand({ name: "fleet-status", type: LaunchType.UserInitiated })}
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
