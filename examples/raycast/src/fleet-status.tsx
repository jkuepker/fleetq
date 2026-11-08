import { Action, ActionPanel, Color, Icon, launchCommand, LaunchType, List } from "@raycast/api";
import {
  bookedLine,
  elapsed,
  hostLine,
  isMine,
  jobTitle,
  overdue,
  summarize,
  TOKEN_HELP,
  useFleetStatus,
} from "./fleet";

function hint(message: string): string {
  if (message.startsWith("No read-only token")) return "";
  if (/^40[13] /.test(message)) return TOKEN_HELP;
  return "Check the Server URL preference, or ~/.config/fleetq/url.";
}

export default function Command() {
  const { data, isLoading, error, revalidate, url } = useFleetStatus();

  const refresh = (
    <Action
      title="Refresh"
      icon={Icon.ArrowClockwise}
      shortcut={{ modifiers: ["cmd"], key: "r" }}
      onAction={revalidate}
    />
  );
  const models = (
    <Action
      title="Open Fleet Models"
      icon={Icon.Stars}
      shortcut={{ modifiers: ["cmd"], key: "m" }}
      onAction={() => launchCommand({ name: "fleet-models", type: LaunchType.UserInitiated })}
    />
  );

  if (error && !data) {
    return (
      <List>
        <List.EmptyView
          icon={Icon.ExclamationMark}
          title={`Could not read ${url}`}
          description={`${error.message}\n${hint(error.message)}`}
          actions={<ActionPanel>{refresh}</ActionPanel>}
        />
      </List>
    );
  }

  const now = data?.now ?? Date.now() / 1000;
  const { hosts, running, queued, booked, attention, stale, staleText, idle } = summarize(data, error);

  return (
    <List isLoading={isLoading} navigationTitle="Fleet Status">
      {data && (
        <List.Section title="Queue">
          <List.Item
            icon={
              stale
                ? { source: Icon.Clock, tintColor: Color.SecondaryText }
                : idle
                  ? { source: Icon.CheckCircle, tintColor: Color.Green }
                  : Icon.List
            }
            title={idle ? "Empty queue" : `${running.length} running · ${queued.length} queued`}
            subtitle={stale ? staleText : idle ? "Fleet is idle" : running.map(jobTitle).join("  ·  ")}
            actions={
              <ActionPanel>
                {refresh}
                {models}
              </ActionPanel>
            }
          />
        </List.Section>
      )}
      {attention.length > 0 && (
        <List.Section title="Attention">
          {attention.map((p) => (
            <List.Item
              key={p.text}
              icon={{ source: Icon.Warning, tintColor: p.level === "red" ? Color.Red : Color.Orange }}
              title={p.text}
              actions={
                <ActionPanel>
                  <Action.CopyToClipboard title="Copy Text" content={p.text} />
                  {refresh}
                </ActionPanel>
              }
            />
          ))}
        </List.Section>
      )}
      {booked.length > 0 && (
        <List.Section title="Booked">
          {booked.map((job) => (
            <List.Item
              key={job.id}
              icon={{ source: Icon.Calendar, tintColor: Color.Purple }}
              title={jobTitle(job)}
              subtitle={bookedLine(job)}
              accessories={[isMine(job) ? { tag: { value: "yours", color: Color.Blue } } : { text: job.owner }]}
              actions={
                <ActionPanel>
                  <Action.CopyToClipboard title="Copy Job ID" content={String(job.id)} />
                  {refresh}
                </ActionPanel>
              }
            />
          ))}
        </List.Section>
      )}
      <List.Section title="Fleet">
        {hosts.map((host) => {
          const job = host.running[0];
          const offline = host.online === false;
          const bad = offline || host.services.some((s) => s.warn);
          const odd = host.services.some((s) => s.status !== "up");
          const accessories: List.Item.Accessory[] = [];
          // Elapsed time is its own accessory: at the end of the subtitle a long label cut it off.
          const ran = job ? elapsed(job, now) : "";
          if (job && isMine(job)) {
            accessories.push({ tag: { value: "yours", color: Color.Blue }, tooltip: job.owner });
          }
          if (job && ran) {
            accessories.push({
              icon: Icon.Clock,
              text: { value: ran, color: overdue(job, now) ? Color.Red : undefined },
              tooltip: overdue(job, now) ? "Running longer than its --expect" : "Running for",
            });
          }
          if (host.queued.length > 0) {
            accessories.push({ tag: { value: `${host.queued.length} queued`, color: Color.Orange } });
          }
          if (host.running.length > 1) {
            accessories.push({ tag: { value: `+${host.running.length - 1} running`, color: Color.Blue } });
          }
          for (const s of host.services.filter((s) => s.status !== "up")) {
            accessories.push({
              tag: { value: `${s.name} ${s.status}`, color: s.warn ? Color.Red : Color.Orange },
              tooltip: s.text,
            });
          }
          accessories.push(
            offline
              ? { tag: { value: "OFFLINE", color: Color.Red } }
              : { text: `${Math.round(host.free_gb)}/${Math.round(host.capacity_gb)} GB free` },
          );

          return (
            <List.Item
              key={host.name}
              icon={{
                source: Icon.CircleFilled,
                tintColor: bad ? Color.Red : odd ? Color.Orange : job ? Color.Yellow : Color.Green,
              }}
              title={host.name}
              subtitle={job ? jobTitle(job) : hostLine(host, now)}
              keywords={[host.machine, ...host.running.map((j) => j.label), ...host.services.map((s) => s.name)]}
              accessories={accessories}
              actions={
                <ActionPanel>
                  {refresh}
                  {job && <Action.CopyToClipboard title="Copy Running Job ID" content={String(job.id)} />}
                  {job && <Action.CopyToClipboard title="Copy Running Job Label" content={job.label} />}
                  <Action.CopyToClipboard
                    title="Copy Queued Jobs"
                    content={host.queued.map((j) => `${j.position ?? "?"}. #${j.id} ${j.label}`).join("\n") || "none"}
                  />
                  {models}
                </ActionPanel>
              }
            />
          );
        })}
      </List.Section>
    </List>
  );
}
