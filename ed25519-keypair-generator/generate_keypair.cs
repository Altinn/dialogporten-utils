#:package NSec.Cryptography@25.4.0

using System.Diagnostics;
using NSec.Cryptography;

using var primaryKeyPair = Key.Create(SignatureAlgorithm.Ed25519, new KeyCreationParameters { ExportPolicy = KeyExportPolicies.AllowPlaintextExport });
var primaryPublicKey = primaryKeyPair.Export(KeyBlobFormat.RawPublicKey);
var primaryPrivateKey = primaryKeyPair.Export(KeyBlobFormat.RawPrivateKey);

using var secondaryKeyPair = Key.Create(SignatureAlgorithm.Ed25519, new KeyCreationParameters { ExportPolicy = KeyExportPolicies.AllowPlaintextExport });
var secondaryPublicKey = secondaryKeyPair.Export(KeyBlobFormat.RawPublicKey);
var secondaryPrivateKey = secondaryKeyPair.Export(KeyBlobFormat.RawPrivateKey);

var hasSetFlag = false;
string? setProjectPath = null;

for (var i = 0; i < args.Length; i++)
{
    var argument = args[i];
    if (argument.StartsWith("--set=", StringComparison.Ordinal))
    {
        hasSetFlag = true;
        setProjectPath = argument["--set=".Length..];
        break;
    }

    if (argument == "--set")
    {
        hasSetFlag = true;
        if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
        {
            setProjectPath = args[i + 1];
        }
        break;
    }
}

var keysValues = new Dictionary<string, string>
{
    { "Application:Dialogporten:Ed25519KeyPairs:Primary:Kid", "dev-primary-signing-key" },
    { "Application:Dialogporten:Ed25519KeyPairs:Primary:PublicComponent", Base64UrlEncode(primaryPublicKey) },
    { "Application:Dialogporten:Ed25519KeyPairs:Primary:PrivateComponent", Base64UrlEncode(primaryPrivateKey) },
    { "Application:Dialogporten:Ed25519KeyPairs:Secondary:Kid", "dev-secondary-signing-key" },
    { "Application:Dialogporten:Ed25519KeyPairs:Secondary:PublicComponent", Base64UrlEncode(secondaryPublicKey) },
    { "Application:Dialogporten:Ed25519KeyPairs:Secondary:PrivateComponent", Base64UrlEncode(secondaryPrivateKey) }
};

if (hasSetFlag)
{
    if (string.IsNullOrWhiteSpace(setProjectPath))
    {
        Console.Error.WriteLine("Missing project path for --set.");
        Console.Error.WriteLine("Usage: dotnet run ed25519-keypair-generator/generate_keypair.cs -- --set <path-to-runtime-project>");
        Console.Error.WriteLine("   or: dotnet run ed25519-keypair-generator/generate_keypair.cs -- --set=<path-to-runtime-project>");
        Environment.Exit(1);
    }

    // Set the keys immediately
    foreach (var (key, value) in keysValues)
    {
        Process.Start("dotnet", $"user-secrets set -p \"{setProjectPath}\" \"{key}\" \"{value}\"")?.WaitForExit();
    }
}
else
{
    // Print the commands to set the keys
    const string projectPathPlaceholder = "<path-to-runtime-project>";
    Console.WriteLine("To set the keys as user secrets, run the following commands, or supply --set <path> to run them automatically:");
    foreach (var (key, value) in keysValues)
    {
        Console.WriteLine($"dotnet user-secrets set -p \"{projectPathPlaceholder}\" \"{key}\" \"{value}\"");
    }
}

Console.WriteLine();
Console.WriteLine("For the keys to be used, set \"UseLocalDevelopmentCompactJwsGenerator\" to false in appsettings.Development.json");

static string Base64UrlEncode(byte[] input)
{
    return Convert.ToBase64String(input).Replace("+", "-").Replace("/", "_").TrimEnd('=');
}
