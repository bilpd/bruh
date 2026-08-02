# Security audit

## Result

No obvious credential stealer, token grabber, hidden downloader, `exec`/`eval`, subprocess launcher, or obfuscated payload was found in the inspected source.

The original repository was still unsafe to run unchanged.

## Removed

- `auto_deposits_and_withdraws_script.lua`: executor code that hooks a client network function, reads and writes executor workspace files, sends mail automatically, claims all mail, and can move account assets without an interactive confirmation.
- `requirements.bat`: attempted to install Python standard-library names such as `json`, `random`, `time`, `os`, and `math` from PyPI. That creates an unnecessary supply-chain and package-shadowing risk.
- Legacy gambling engine and JSON state files: contained unrelated games, brittle file-based balances, blocking calls inside async handlers, and hardcoded privileged Discord user IDs.
- `.idea/`: local IDE metadata that should not be committed.

## Remaining backend

`server_websocket.py` only accepts authenticated event reports, validates item data, maintains a local ledger, and sends optional Discord webhook embeds. It does not control Roblox clients or transfer items.

## Required precautions

- Keep `.env` out of Git.
- Use a unique password of at least 12 characters.
- Rotate any Discord webhook or token that has ever been pasted publicly.
- Put the service behind a firewall or reverse proxy before exposing it to the internet.
- Treat client-reported deposit and withdrawal events as untrusted unless independently confirmed by an authoritative server.
