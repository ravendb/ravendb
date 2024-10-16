using System;
using System.Net.Http;

namespace Raven.Client.Http;

internal interface IRavenHttpClientFactory
{
    bool CanCacheHttpClient { get; }

    HttpClient GetHttpClient(HttpClientCacheKey key, Func<HttpClientHandler, HttpClient> createHttpClient);

    bool TryRemoveHttpClient(HttpClientCacheKey key, bool force = false);
}
