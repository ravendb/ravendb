using IOPath = System.IO.Path;

namespace Raven.Quill.Logging;

public sealed record QuillLogOptions
{
    internal const string ConfigPathVariable = "RAVEN_QUILL_LOGS_CONFIG_PATH";
    internal const string PathVariable = "RAVEN_QUILL_LOGS_PATH";
    internal const string AuditPathVariable = "RAVEN_QUILL_SECURITY_AUDITLOG_PATH";

    private const string DefaultConfigDirectory = "/var/lib/quill";
    private const string DefaultLogDirectoryName = "logs";

    public string? ConfigPath { get; init; } = DefaultConfigPath;

    public string Path { get; init; } = DefaultDirectory;

    public string AuditPath { get; init; } = DefaultDirectory;

    private static string DefaultConfigPath => IOPath.Combine(DefaultConfigDirectory, QuillLogging.ConfigFileName);

    private static string DefaultDirectory => IOPath.Combine(AppContext.BaseDirectory, DefaultLogDirectoryName);

    public static QuillLogOptions FromEnvironment()
    {
        var configPath = Environment.GetEnvironmentVariable(ConfigPathVariable);

        return new QuillLogOptions
        {
            ConfigPath = configPath is null
                ? DefaultConfigPath
                : string.IsNullOrWhiteSpace(configPath) ? null : configPath,
            Path = Directory(PathVariable),
            AuditPath = Directory(AuditPathVariable),
        };
    }

    private static string Directory(string variable) =>
        Environment.GetEnvironmentVariable(variable) is { } value && string.IsNullOrWhiteSpace(value) == false
            ? value
            : DefaultDirectory;
}
