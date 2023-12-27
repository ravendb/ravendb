using System;
using System.Net.Http;
using System.Net.WebSockets;
using Raven.Client;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Conventions;
using Raven.Server.Utils;

namespace Raven.Server.Extensions
{
    public static class HttpExtensions
    {
        public static HttpRequestMessage WithConventions(this HttpRequestMessage request, DocumentConventions conventions)
        {
            if (request == null)
                throw new ArgumentNullException(nameof(request));
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            request.Version = conventions.HttpVersion;
            if (conventions.HttpVersionPolicy.HasValue)
                request.VersionPolicy = conventions.HttpVersionPolicy.Value;

            return request;
        }

        public static HttpClient WithConventions(this HttpClient httpClient, DocumentConventions conventions)
        {
            if (httpClient == null)
                throw new ArgumentNullException(nameof(httpClient));
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            httpClient.DefaultRequestVersion = conventions.HttpVersion;
            if (conventions.HttpVersionPolicy.HasValue)
                httpClient.DefaultVersionPolicy = conventions.HttpVersionPolicy.Value;

            return httpClient;
        }

        public static RavenHttpClient WithConventions(this RavenHttpClient httpClient, DocumentConventions conventions)
        {
            if (httpClient == null)
                throw new ArgumentNullException(nameof(httpClient));
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            httpClient.DefaultRequestVersion = conventions.HttpVersion;
            if (conventions.HttpVersionPolicy.HasValue)
                httpClient.DefaultVersionPolicy = conventions.HttpVersionPolicy.Value;

            return httpClient;
        }

        public static ClientWebSocket WithConventions(this ClientWebSocket clientWebSocket, DocumentConventions conventions)
        {
            if (clientWebSocket == null)
                throw new ArgumentNullException(nameof(clientWebSocket));
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));

            clientWebSocket.Options.HttpVersion = conventions.HttpVersion;
            if (conventions.HttpVersionPolicy.HasValue)
                clientWebSocket.Options.HttpVersionPolicy = conventions.HttpVersionPolicy.Value;

            return clientWebSocket;
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

        public static bool IsFromOrchestrator(this HttpRequest request)
        {
            return GetBoolFromHeaders(request, Constants.Headers.Sharded) ?? false;
        }

        public static bool IsFromClientApi(this HttpRequest request)
        {
            return request.Headers.ContainsKey(Constants.Headers.ClientVersion);
        }

        private static bool? GetBoolFromHeaders(HttpRequest request, string name)
        {
            var headers = request.Headers[name];
            if (headers.Count == 0)
                return null;


            var raw = headers[0][0] == '\"'
                ? headers[0].AsSpan().Slice(1, headers[0].Length - 2)
                : headers[0].AsSpan();

            var success = bool.TryParse(raw, out var result);

            if (success)
                return result;

            return null;
        }
    }
}
