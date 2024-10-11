using System;
using System.Net.Http;
using System.Reflection;
using Raven.Client.Http;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_22436 : NoDisposalNeeded
{
    public RavenDB_22436(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void HttpClientFactories_ShouldConfigureHttpClient_Properly()
    {
        var getHandler = typeof(HttpMessageInvoker).GetField("_handler", BindingFlags.Instance | BindingFlags.NonPublic);
        if (getHandler == null)
            throw new InvalidOperationException("Could not get underlying handler field from HttpClient.");

        var getUnderlyingHandler = typeof(HttpClientHandler).GetField("_underlyingHandler", BindingFlags.Instance | BindingFlags.NonPublic)
                                   ?? typeof(HttpClientHandler).GetField("_socketsHttpHandler", BindingFlags.Instance | BindingFlags.NonPublic);

        if (getUnderlyingHandler == null)
            throw new InvalidOperationException("Could not get underlying handler field from HttpClientHandler.");

        var key = new HttpClientCacheKey(certificate: null, useCompression: true, hasExplicitlySetCompressionUsage: false, pooledConnectionLifetime: TimeSpan.FromMinutes(3), pooledConnectionIdleTimeout: TimeSpan.FromMinutes(1), typeof(HttpClient));

        var httpClient = DefaultRavenHttpClientFactory.Instance.GetHttpClient(key, handler => new HttpClient(handler));
        AssertHttpClient(httpClient);

        var handler = (HttpClientHandler)getHandler.GetValue(httpClient);
        var socketsHttpHandler = (SocketsHttpHandler)getUnderlyingHandler.GetValue(handler);
        AssertHttpHandler(socketsHttpHandler);

        var httpClient2 = RavenServerHttpClientFactory.Instance.GetHttpClient(key, handler => new HttpClient(handler));
        AssertHttpClient(httpClient2);

        var handler2 = (DelegatingHandler)getHandler.GetValue(httpClient2);
        handler2 = (DelegatingHandler)handler2.InnerHandler;
        handler2 = (DelegatingHandler)handler2.InnerHandler;
        handler = (HttpClientHandler)handler2.InnerHandler;
        socketsHttpHandler = (SocketsHttpHandler)getUnderlyingHandler.GetValue(handler);
        AssertHttpHandler(socketsHttpHandler);

        return;

        void AssertHttpClient(HttpClient client)
        {
            Assert.Equal(RequestExecutor.GlobalHttpClientTimeout, client.Timeout);
        }

        void AssertHttpHandler(SocketsHttpHandler h)
        {
            Assert.Equal(key.PooledConnectionIdleTimeout, h.PooledConnectionIdleTimeout);
            Assert.Equal(key.PooledConnectionLifetime, h.PooledConnectionLifetime);
            Assert.Equal(RequestExecutor.DefaultConnectionLimit, h.MaxConnectionsPerServer);
        }
    }
}
