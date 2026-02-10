# Ed25519 Key Pair Generator

This script generates two Ed25519 key pairs (primary and secondary) for Dialogporten dialog token signing and verification in local development.

It is intended for use with the Dialogporten dialog token issuer feature described in the Altinn documentation:
https://docs.altinn.studio/en/dialogporten/getting-started/authorization/dialog-tokens/

## Prerequisites

- .NET 10 SDK installed (`dotnet --version`)

## Usage

From the repository root:

```bash
dotnet run ed25519-keypair-generator/generate_keypair.cs
```

This prints `dotnet user-secrets set` commands with generated values.

To apply the generated values directly, provide the runtime project path explicitly:

```bash
dotnet run ed25519-keypair-generator/generate_keypair.cs -- --set <path-to-runtime-project>
```

or

```bash
dotnet run ed25519-keypair-generator/generate_keypair.cs -- --set=<path-to-runtime-project>
```

`<path-to-runtime-project>` is the path accepted by `dotnet user-secrets -p`, typically a `.csproj` file.

## What Gets Set

The script generates and sets:

- `Application:Dialogporten:Ed25519KeyPairs:Primary:Kid`
- `Application:Dialogporten:Ed25519KeyPairs:Primary:PublicComponent`
- `Application:Dialogporten:Ed25519KeyPairs:Primary:PrivateComponent`
- `Application:Dialogporten:Ed25519KeyPairs:Secondary:Kid`
- `Application:Dialogporten:Ed25519KeyPairs:Secondary:PublicComponent`
- `Application:Dialogporten:Ed25519KeyPairs:Secondary:PrivateComponent`

For the keys to be used, set `UseLocalDevelopmentCompactJwsGenerator` to `false` in `appsettings.Development.json`.
