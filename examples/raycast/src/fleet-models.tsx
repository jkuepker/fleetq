import { Action, ActionPanel, Color, Icon, List } from "@raycast/api";
import { aliasOf, harnessName, Model, TOKEN_HELP, useFleetModels } from "./fleet";

function context(m: Model): List.Item.Accessory {
  if (!m.context) return { text: "context ?", tooltip: "The server does not report its context size" };
  return { text: `${Math.round(m.context / 1024)}K`, tooltip: `${m.context} tokens of context` };
}

export default function Command() {
  const { data, isLoading, error, revalidate, url } = useFleetModels();
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
          title={`Could not read ${url}`}
          description={`${error.message}\n${/^40[13] /.test(error.message) ? TOKEN_HELP : ""}`}
          actions={<ActionPanel>{refresh}</ActionPanel>}
        />
      </List>
    );
  }

  const models = data?.models ?? [];
  // One section per server URL, usable ones first, in the order the server lists them.
  const urls = [...new Set(models.map((m) => m.url))].sort(
    (a, b) =>
      Number(models.some((m) => m.url === b && m.usable)) - Number(models.some((m) => m.url === a && m.usable)),
  );

  return (
    <List isLoading={isLoading} navigationTitle="Fleet Models" searchBarPlaceholder="Filter by model, host or role">
      {urls.map((u) => {
        const here = models.filter((m) => m.url === u);
        return (
          <List.Section key={u} title={here[0].host} subtitle={u}>
            {here.map((m) => {
              const alias = aliasOf(m, models);
              const name = harnessName(alias ?? m);
              const accessories: List.Item.Accessory[] = [];
              if (alias) accessories.push({ tag: { value: `alias of ${alias.model}`, color: Color.SecondaryText } });
              for (const cap of m.capabilities.filter((c) => c !== "text")) {
                accessories.push({ tag: { value: cap, color: Color.Purple } });
              }
              accessories.push(context(m));
              if (!m.usable) accessories.push({ tag: { value: m.state, color: Color.Orange } });
              return (
                <List.Item
                  key={`${m.url} ${m.model}`}
                  icon={{ source: Icon.CircleFilled, tintColor: m.usable ? Color.Green : Color.SecondaryText }}
                  title={m.model}
                  subtitle={m.root ?? undefined}
                  keywords={[m.host, m.root ?? "", ...(m.capabilities ?? [])]}
                  accessories={accessories}
                  actions={
                    <ActionPanel>
                      <Action.CopyToClipboard title="Copy Agent Model Name" content={name} />
                      <Action.CopyToClipboard title="Copy Served Name" content={m.model} />
                      <Action.CopyToClipboard
                        title="Copy Base URL"
                        content={m.url}
                        shortcut={{ modifiers: ["cmd", "shift"], key: "c" }}
                      />
                      {refresh}
                    </ActionPanel>
                  }
                />
              );
            })}
          </List.Section>
        );
      })}
    </List>
  );
}
