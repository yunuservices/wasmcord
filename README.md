# wasmcord

WASM-based Discord bot framework. Plugins run as isolated WebAssembly components and can be hot-reloaded while the bot is running.

## Features

- WASM plugin runtime with per-plugin CPU/memory limits and permissions
- Discord gateway event dispatch + outbound commands
- REST helpers: HTTP, attachments, components, interactions, modals
- Voice support via Songbird
- Inter-plugin event bus
- Hot reload and watchdog (auto-unload failing plugins)
- Plugin manifests with dependencies and semver resolution
- FJALL-backed key-value storage
- Discord rate limiting and retry

## Requirements

- Rust 1.85+ (2024 edition)
- Windows, Linux, or macOS
- (Optional) `libopus-dev` / `opus` for voice features

## Quick Start

```bash
git clone https://github.com/yunuservices/wasmcord
cd wasmcord

cp .env.example .env
# Edit .env and set DISCORD_TOKEN='Bot YOUR_TOKEN'

cargo run --release
```

The bot loads all `.wasm` files from the `plugins/` directory on startup.

## .env

```env
DISCORD_TOKEN=Bot YOUR_BOT_TOKEN_HERE
DATA_DIR=./data
PLUGIN_DIR=./plugins
RUST_LOG=info
```

## Writing a Plugin

### Manifest

`plugins/hello.toml`:

```toml
[plugin]
name = "hello"
version = "0.1.0"
abi_version = 1

[permissions]
http = true
```

### Host Functions

- `http-request`
- `send-channel-message-with-attachments`
- `send-channel-message-with-components`
- `reply-to-interaction`, `edit-interaction-message`, `show-modal`
- `application-id`, `update-presence`, `request-guild-members`
- `join-voice-channel`, `leave-voice-channel`, `play-audio-url`, `stop-audio`, `pause-audio`, `resume-audio`, `skip-audio`, `set-volume`
- `bus-publish`, `bus-subscribe`
- `kv-get`, `kv-set`, `fs-read`, `fs-write`, `get-env`, `log`

### Example

See `example-plugin/src/lib.rs` for a slash command implementation. Build with:

```bash
cd example-plugin
cargo build --target wasm32-wasip2 --release
```

Copy `target/wasm32-wasip2/release/ping_plugin.wasm` into `plugins/`.

## Manifest Fields

```toml
[plugin]
name = "music"
version = "0.2.0"
abi_version = 1

[dependencies]
queue = { version = ">=0.1.0" }

[permissions]
http = true
kv = true
bus = true

[limits]
max_memory_bytes = 67108864
```

- `abi_version` — blocks old plugins when the host WIT changes
- `dependencies` — required/optional plugin deps with semver
- `permissions` — enabled host functions
- `limits` — WASM resource caps

## Voice

1. Join a channel:
   ```
   join-voice-channel(guild-id, channel-id, self-mute, self-deaf)
   ```
2. Play audio:
   ```
   play-audio-url(guild-id, "https://example.com/audio.mp3")
   ```

## Storage

KV is stored by `fjall` under `./data/plugin-kv` (or `DATA_DIR`).

## Hot Reload

Replace a `.wasm` in `plugins/` while the bot is running. The bot loads the new version, shuts down the old one if successful, or keeps the old one if the new version fails.

## Architecture Notes

- Each plugin has its own `workspace/`
- Plugins communicate only via the event bus
- 5 consecutive failures/traps/timeouts trigger automatic unload

## Production Tips

- Restrict `.env` permissions
- Mount `DATA_DIR` to persistent storage
- Use `RUST_LOG=info` or `warn`
- Consider separate voice workers for very high voice concurrency

## License

This project is licensed under the [MIT License](LICENSE).
