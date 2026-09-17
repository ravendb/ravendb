using System;
using NLog;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Quill.Logging;

/// <summary>
/// The manager Quill hands to <see cref="RavenLogManager.Set"/>: <see cref="RavenNLogLogManager"/> in
/// every respect but the "Audit" logger, which comes back gated so RavenLogManager's own audit flag
/// answers with whether audit is switched on rather than with whether the always-present rule matches.
/// </summary>
internal sealed class QuillNLogLogManager : IRavenLogManager
{
    private const string AuditLoggerName = "Audit";

    public static readonly QuillNLogLogManager Instance = new();

    private readonly IRavenLogManager _inner = RavenNLogLogManager.Instance;

    private QuillNLogLogManager()
    {
    }

    public IRavenLogger GetLogger(string name) =>
        string.Equals(name, AuditLoggerName, StringComparison.Ordinal)
            ? new AuditGatedRavenLogger(LogManager.GetLogger(name))
            : _inner.GetLogger(name);

    public event EventHandler<RavenLoggingConfigurationChangedEventArgs> ConfigurationChanged
    {
        add => _inner.ConfigurationChanged += value;
        remove => _inner.ConfigurationChanged -= value;
    }

    public void Shutdown() => _inner.Shutdown();
}
