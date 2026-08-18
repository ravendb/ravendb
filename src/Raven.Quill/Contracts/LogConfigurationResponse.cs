using Raven.Client.ServerWide.Operations.Logs;
using LogLevel = Sparrow.Logging.LogLevel;
using RavenLogsConfiguration = Raven.Client.ServerWide.Operations.Logs.LogsConfiguration;

namespace Raven.Quill.Contracts;

public sealed record LogConfigurationResponse(
    RavenLogsConfiguration Logs,
    AuditLogsConfiguration AuditLogs,
    MicrosoftLogsConfiguration MicrosoftLogs,
    bool CanPersist);

public sealed record UpdateLogConfigurationRequest(
    LogsUpdate? Logs = null,
    MicrosoftLogsConfiguration? MicrosoftLogs = null,
    bool Persist = false)
{
    public bool IsEmpty => Logs is null && MicrosoftLogs is null;
}

public sealed record LogsUpdate(
    string? Path = null,
    LogLevel MinLevel = LogLevel.Info);
