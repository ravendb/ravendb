using System;
using Raven.Client;
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
            return request.Scheme + "://" + request.Host + path + request.QueryString;
        }

        public static string ExtractNodeUrlFromRequest(HttpRequest request)
        {
            string requestUrl = null;

            if (request != null)
            {
                var uriBuilder = new UriBuilder(
                    request.Scheme,
                    request.Host.Host,
                    request.Host.Port ?? (request.IsHttps ? 443 : 80),
                    string.Empty);
                requestUrl = uriBuilder.ToString().TrimEnd('/');
            }

            return requestUrl;
        }

        public static string GetClientRequestedNodeUrl(this HttpRequest request)
        {
            if (request.Query.TryGetValue("localUrl", out var localUrl) && localUrl.Count > 0)
            {
                return localUrl[0];
            }
            return ExtractNodeUrlFromRequest(request);
        }

        public static bool IsFromStudio(this HttpRequest request)
        {
            return request.Headers.ContainsKey(Constants.Headers.StudioVersion);
        }
    }
}
