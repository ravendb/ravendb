using System;
using JetBrains.Annotations;
using NLog;

namespace Raven.Server.Logging;

public sealed class RavenAuditLogger
{
    private readonly Logger _logger;

    public RavenAuditLogger([NotNull] Logger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public bool IsAuditEnabled => _logger.IsInfoEnabled;

    public void Audit(string message)
    {
        _logger.Info(message);
    }
}
