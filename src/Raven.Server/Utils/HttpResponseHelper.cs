using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;

namespace Raven.Server.Utils;

public static class HttpResponseHelper
{
    private static readonly HashSet<string> HeadersToIgnore = new HashSet<string>
    {
        "Vary",
        "Date",
        "Server",
        Constants.Headers.TransferEncoding
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CopyStatusCode(HttpResponseMessage from, HttpResponse to)
    {
        to.StatusCode = (int)from.StatusCode;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CopyHeaders(HttpResponseMessage from, HttpResponse to)
    {
        to.Headers.Clear();

        foreach (var header in from.Headers)
        {
            if (HeadersToIgnore.Contains(header.Key))
                continue;

            to.Headers.Add(header.Key, header.Value.ToArray());
        }

        foreach (var header in from.Content.Headers)
        {
            if (HeadersToIgnore.Contains(header.Key))
                continue;

            to.Headers.Add(header.Key, header.Value.ToArray());
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Task CopyContentAsync(HttpResponseMessage from, HttpResponse to)
    {
        return from.Content.CopyToAsync(to.Body);
    }
}
