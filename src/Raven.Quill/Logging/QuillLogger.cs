using System.Security.Claims;
using System.Text;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Quill.Logging;

/// <summary>
/// What the endpoints and services inject: a <see cref="RavenLogger"/> for the category, plus the audit
/// log. Both come from <see cref="RavenLogManager"/>, so a Quill line carries the same Resource column a
/// Raven.Server one does. Nothing bridges <c>ILogger</c> to NLog, so this is the only way into the log -
/// the framework's own <c>Microsoft.*</c> and <c>System.*</c> output goes nowhere.
/// <para>
/// Messages are interpolated strings, as they are in RavenDB, and there is deliberately no overload that
/// takes arguments: NLog would read <c>{Placeholder}</c> as a message template and quote every string it
/// substituted. Nothing reads the properties a template would produce - the layout renders the message
/// and takes Resource, Component and Data from the logger, not the call.
/// </para>
/// </summary>
public sealed class QuillLogger<TCategory>
{
    private const string NoPrincipal = "no principal";
    private const string UnknownIp = "unknown";
    private const string InternalActor = "internal";

    private readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForQuill<TCategory>();

    private readonly RavenAuditLogger _audit = RavenLogManager.Instance.GetAuditLoggerForQuill();

    public bool AuditEnabled => _audit.IsAuditEnabled;

    /// <summary>
    /// Writes one audit record: who, from where, and what they did. The same three-part line RavenDB's
    /// <c>RequestHandler.LogAuditForInternal</c> builds - it lives here rather than on a request handler
    /// because Quill's endpoints are minimal APIs with no shared base to hang it on.
    /// <para>
    /// Attribution is per-key, not per-person: a line names a source IP and an authentication method. The
    /// IP is the real client's because UseForwardedHeaders runs ahead of UseAuthentication, so what
    /// reaches here is not the nginx loopback address. A null <paramref name="context"/> means the
    /// appliance itself acted, with no request behind it.
    /// </para>
    /// </summary>
    public void Audit(string action, string target, HttpContext? context, ClaimsPrincipal? principal = null)
    {
        var builder = new StringBuilder();

        builder.Append(context is null
            ? InternalActor
            : context.Connection.RemoteIpAddress?.ToString() ?? UnknownIp);
        builder.Append(", ");
        builder.Append(DescribePrincipal(principal ?? context?.User));
        builder.Append(", ");
        builder.Append(action);
        builder.Append(' ');
        builder.Append(target);

        _audit.Audit(builder.ToString());
    }

    public bool IsDebugEnabled => _logger.IsDebugEnabled;

    public bool IsInfoEnabled => _logger.IsInfoEnabled;

    public bool IsWarnEnabled => _logger.IsWarnEnabled;

    public bool IsErrorEnabled => _logger.IsErrorEnabled;

    public void Debug(string message) => _logger.Debug(message);

    public void Debug(Exception exception, string message) => _logger.Debug(message, exception);

    public void Info(string message) => _logger.Info(message);

    public void Info(Exception exception, string message) => _logger.Info(message, exception);

    public void Warn(string message) => _logger.Warn(message);

    public void Warn(Exception exception, string message) => _logger.Warn(message, exception);

    public void Error(string message) => _logger.Error(message);

    public void Error(Exception exception, string message) => _logger.Error(message, exception);

    private static string DescribePrincipal(ClaimsPrincipal? principal) =>
        principal?.Identity is { IsAuthenticated: true } identity
            ? $"{identity.AuthenticationType} [{identity.Name ?? "unnamed"}]"
            : NoPrincipal;
}
