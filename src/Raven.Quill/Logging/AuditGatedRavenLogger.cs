using NLog;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Quill.Logging;

/// <summary>
/// The "Audit" logger, reporting Info-enabled only when audit is actually switched on. NLog's rule for it
/// is installed either way - it has to be, so its Final flag keeps audit records out of the normal log -
/// which leaves the bare logger Info-enabled even when nothing is being audited.
/// The member is re-implemented against <see cref="IRavenLogger"/> rather than overridden, because that
/// is the only way <see cref="RavenLogManager"/> reads it and it keeps RavenLogger's own property
/// non-virtual.
/// </summary>
internal sealed class AuditGatedRavenLogger : RavenLogger, IRavenLogger
{
    internal AuditGatedRavenLogger(Logger logger) : base(logger)
    {
    }

    bool IRavenLogger.IsInfoEnabled => RavenLogManager.GlobalIsAuditEnabled && IsInfoEnabled;
}
