# Notifications to a self-hosted ntfy

fleetq's `[[notify]]` entries can post to a [ntfy](https://ntfy.sh) server you run yourself.
This page sets one up so that nothing is public: deny-all access, one publish-only token for
fleetq, a read-only login for your phone, all reachable over Tailscale. The README's
"Notifications" section covers the rest of `[[notify]]`.

Host names, topics and tokens below are examples; use your own.

## 1. Run ntfy with deny-all access

`/etc/ntfy/server.yml` on the machine that hosts it (any box on your tailnet):

```yaml
base-url: "http://ntfy-box:2586"        # the address your phone will use: its Tailscale name
listen-http: ":2586"
auth-file: "/var/lib/ntfy/user.db"
auth-default-access: "deny-all"         # nobody reads or writes anything without an account
upstream-base-url: "https://ntfy.sh"    # iOS only, see below
```

Create the accounts. fleetq gets a user that may only publish, to one topic, and a token for
it; you get a user that may only read that topic:

```bash
ntfy user add fleetq                                  # prompts for a password you will not need
ntfy access fleetq fleet-alerts write-only
ntfy token add fleetq                                 # prints tk_..., the token fleetq sends
ntfy user add alice
ntfy access alice fleet-alerts read-only
```

## 2. Give fleetq the token

Put the token in a file readable only by the fleetq user. Never in the roster itself: fleetq
refuses an inline `token`.

```bash
install -m 600 -o fleetq -g fleetq /dev/stdin /etc/fleetq/ntfy.token <<< "tk_..."
```

```toml
[[notify]]
url = "http://ntfy-box:2586/fleet-alerts"
format = "ntfy"                                       # see the note below
token_file = "/etc/fleetq/ntfy.token"
kinds = ["overdue", "stale", "unclaimed", "overrun", "lapsed", "not_restored", "wedged"]
```

- **`format = "ntfy"` is required** for a self-hosted server. fleetq only recognises `ntfy.sh`
  itself; any other host would get the JSON body, which ntfy would show as raw text.
- fleetq sends `Authorization: Bearer <token>` to this entry's URL, and to nothing else. The
  token is never written to the log, to events, or to API responses.
- The fleetq host must reach `ntfy-box`: it needs Tailscale too, or put both on a network
  they share. Environment proxies are ignored.
- `fleetqd --config /etc/fleetq/fleet.toml --check` fails if the file is missing, empty or
  holds more than one word.

Test it before relying on it (`-H` carries the same token):

```bash
curl -H "Authorization: Bearer tk_..." -H "Title: fleetq test" -d "it works" http://ntfy-box:2586/fleet-alerts
```

## 3. Subscribe on your phone

Install the ntfy app, open Settings, add `http://ntfy-box:2586` as the default server, log in
as `alice`, then subscribe to `fleet-alerts`. The phone has to be on the tailnet to reach it.

**iOS** cannot hold a connection to a self-hosted server in the background, so the server asks
`ntfy.sh` to wake the app: that is the `upstream-base-url` line above. ntfy.sh then sees only a
message ID and a hash of the topic, not the text; the app still fetches the message from
`ntfy-box` over Tailscale. Android needs no `upstream-base-url`.
