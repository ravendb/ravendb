using System.Security.Claims;
using System.Text;
using Raven.Server.Logging;

namespace Raven.Quill.Logging;

internal static class QuillAudit
{
    internal const string NoPrincipal = "no principal";
    private const string UnknownIp = "unknown";
    private const string InternalActor = "internal";

    internal static void Audit(this RavenAuditLogger audit, string action, string target,
        HttpContext? context, ClaimsPrincipal? principal = null) =>
        audit.Audit(BuildLine(action, target, context, principal));

    internal static string BuildLine(string action, string target, HttpContext? context,
        ClaimsPrincipal? principal = null)
    {
        var builder = new StringBuilder();

        builder.Append(context is null ? InternalActor : context.Connection.RemoteIpAddress?.ToString() ?? UnknownIp);
        builder.Append(", ");
        builder.Append(DescribePrincipal(principal ?? context?.User));
        builder.Append(", ");
        builder.Append(action);
        builder.Append(' ');
        builder.Append(target);

        return builder.ToString();
    }

    private static string DescribePrincipal(ClaimsPrincipal? principal) =>
        principal?.Identity is { IsAuthenticated: true } identity
            ? $"{identity.AuthenticationType} [{identity.Name ?? "unnamed"}]"
            : NoPrincipal;
}
