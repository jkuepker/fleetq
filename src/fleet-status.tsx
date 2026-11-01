import { Action, ActionPanel, Color, Icon, List } from "@raycast/api";
import { hostLine, summarize, useFleetStatus } from "./fleet";

export default function Command() {
  const { data, isLoading, error, revalidate } = useFleetStatus();

  const refresh = (
    <Action
      title="Refresh"
      icon={Icon.ArrowClockwise}
      shortcut={{ modifiers: ["cmd"], key: "r" }}
      onAction={revalidate}
    />
  );

  if (error && !data) {
    return (
      <List>
        <List.EmptyView
          icon={Icon.ExclamationMark}
          title="Could not run fleetq"
          description={`${error.message}\nCheck the fleetq Path preference.`}
          actions={<ActionPanel>{refresh}</ActionPanel>}
        />
      </List>
    );
  }

  const now = data?.now ?? Date.now() / 1000;
  const { hosts, running, queued, idle } = summarize(data);

  return (
    <List isLoading={isLoading} navigationTitle="Fleet Status">
      {data && (
        <List.Section title="Queue">
          <List.Item
            icon={idle ? { source: Icon.CheckCircle, tintColor: Color.Green } : Icon.List}
            title={idle ? "Empty queue" : `${running.length} running · ${queued.length} queued`}
            subtitle={idle ? "Fleet is idle" : running.map((j) => `#${j.id} ${j.label}`).join("  ·  ")}
            actions={<ActionPanel>{refresh}</ActionPanel>}
          />
        </List.Section>
      )}
      <List.Section title="Fleet">
        {hosts.map((host) => {
          const job = host.running[0];
          const offline = host.online === false;
          const accessories: List.Item.Accessory[] = [];
          if (host.queued.length > 0) {
            accessories.push({ tag: { value: `${host.queued.length} queued`, color: Color.Orange } });
          }
          if (host.running.length > 1) {
            accessories.push({ tag: { value: `+${host.running.length - 1} running`, color: Color.Blue } });
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
                tintColor: offline ? Color.Red : job ? Color.Yellow : Color.Green,
              }}
              title={host.name}
              subtitle={hostLine(host, now)}
              keywords={[host.machine, ...host.running.map((j) => j.label)]}
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
                </ActionPanel>
              }
            />
          );
        })}
      </List.Section>
    </List>
  );
}
