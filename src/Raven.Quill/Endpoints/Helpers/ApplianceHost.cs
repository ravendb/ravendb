using System.Net;
using Microsoft.AspNetCore.Http;

namespace Raven.Quill.Endpoints.Helpers;

internal static class ApplianceHost
{
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
