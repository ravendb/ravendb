using System;
using System.Net.Http;

namespace Raven.Client.Http;

internal interface IRavenHttpClientFactory
{
    HttpClient GetHttpClient(HttpClientCacheKey key, Func<HttpClientHandler, HttpClient> createHttpClient);

    bool TryRemoveHttpClient(HttpClientCacheKey key, bool force = false);
}
