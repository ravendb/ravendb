using System;
using Microsoft.AspNetCore.Http;

namespace Raven.Server.Extensions
{
    public static class HttpExtensions
    {
        public static string GetHostnameUrl(this HttpRequest request)
        {
            if (string.IsNullOrEmpty(request.Scheme))
                throw new InvalidOperationException("Missing Scheme");
            if (!request.Host.HasValue)
                throw new InvalidOperationException("Missing Host");

            return request.Scheme + "://" + request.Host;
        }

        public static string GetFullUrl(this HttpRequest request)
        {
            if (string.IsNullOrEmpty(request.Scheme))
                throw new InvalidOperationException("Missing Scheme");
            if (!request.Host.HasValue)
                throw new InvalidOperationException("Missing Host");

            string path = (request.PathBase.HasValue || request.Path.HasValue) ? (request.PathBase + request.Path).ToString() : "/";
            return request.Scheme + "://" + request.Host + path + request.Query;
        }
    }
}
