using System.Net;
using Microsoft.AspNetCore.Http;

namespace Raven.AiAppliance.Endpoints.Helpers;

/// <summary>
/// Maps a request host to a sibling appliance subdomain on the same base domain. The appliance's
/// surfaces share one base (db.* / a.* / dashboard.* / api.* / public.*), so one host is derived
/// from another by swapping the leading DNS label — no extra configuration for the SNI model.
/// </summary>
internal static class ApplianceHost
{
    /// <summary>
    /// Returns <paramref name="host"/> with its leading DNS label replaced by <paramref name="subdomain"/>,
    /// preserving the port. Returns the host unchanged when there is no leading label to swap — an IP
    /// address, <c>localhost</c>, or a bare apex — so callers fall back to the original request host.
    /// </summary>
    public static HostString WithSubdomain(HostString host, string subdomain)
    {
        var name = host.Host;
        var dot = name.IndexOf('.');
        if (dot <= 0 || IPAddress.TryParse(name, out _))
            return host;

        var baseDomain = name[(dot + 1)..];
        var swapped = $"{subdomain}.{baseDomain}";
        return host.Port is { } port ? new HostString(swapped, port) : new HostString(swapped);
    }
}
