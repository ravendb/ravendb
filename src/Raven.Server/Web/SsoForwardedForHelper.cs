using System;
using System.Collections.Generic;
using System.Net;
using Microsoft.AspNetCore.Http;
using Raven.Client;

namespace Raven.Server.Web;

internal static class SsoForwardedForHelper
{
    private const int MaxHeaderLength = 8 * 1024;
    private const int MaxHops = 16;

    public static (string ClientIp, string ProxyIp) GetIps(HttpContext context, RavenServer.AuthenticateConnection auth)
    {
        var directIp = context.Connection.RemoteIpAddress?.ToString();

        // Only trust X-Forwarded-For from SSO-authenticated connections
        if (auth?.IsSsoAuthenticated != true)
            return (directIp, null);

        if (context.Request.Headers.TryGetValue(Constants.Headers.XForwardedFor, out var xff))
        {
            if (TrySanitize(xff.ToString(), out var sanitized))
                return (sanitized, directIp);
        }

        return (directIp, null);
    }

    private static bool TrySanitize(string raw, out string sanitized)
    {
        sanitized = null;
        if (string.IsNullOrWhiteSpace(raw) || raw.Length > MaxHeaderLength)
            return false;

        var hops = raw.Split(',');
        var validHops = new List<string>(Math.Min(hops.Length, MaxHops));

        foreach (var hop in hops)
        {
            if (validHops.Count >= MaxHops)
                break;

            if (TryParseIp(hop.Trim(), out var normalized))
                validHops.Add(normalized);
        }

        if (validHops.Count == 0)
            return false;

        sanitized = string.Join(" -> ", validHops);
        return true;
    }

    private static bool TryParseIp(string entry, out string normalized)
    {
        normalized = null;
        if (string.IsNullOrEmpty(entry))
            return false;

        // Handle IPv6 with port: [2001:db8::1]:443
        if (entry[0] == '[')
        {
            var closingBracket = entry.IndexOf(']');
            if (closingBracket < 0)
                return false;
            entry = entry.Substring(1, closingBracket - 1);
        }
        else
        {
            // Handle IPv4 with port: 203.0.113.1:54321
            // (IPv6 has multiple colons so we only strip when the candidate before the last colon is a valid IPv4)
            var lastColon = entry.LastIndexOf(':');
            if (lastColon > 0)
            {
                var candidate = entry.Substring(0, lastColon);
                if (IPAddress.TryParse(candidate, out _))
                    entry = candidate;
            }
        }

        if (IPAddress.TryParse(entry, out var ip) == false)
            return false;

        normalized = ip.ToString();
        return true;
    }
}
