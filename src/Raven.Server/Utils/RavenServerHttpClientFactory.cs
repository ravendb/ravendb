using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Options;
using Raven.Client.Http;

namespace Raven.Server.Utils;

internal class RavenServerHttpClientFactory : IRavenHttpClientFactory
{
    private readonly RavenDynamicHttpClientFactoryConfiguration _configuration;
    private readonly IHttpClientFactory _factory;

    public static readonly RavenServerHttpClientFactory Instance = new();

    private RavenServerHttpClientFactory()
    {
        var services = new ServiceCollection();
        services.AddHttpClient();
        services.AddSingleton<IConfigureOptions<HttpClientFactoryOptions>, RavenDynamicHttpClientFactoryConfiguration>();

        var provider = services.BuildServiceProvider();

        _configuration = (RavenDynamicHttpClientFactoryConfiguration)provider.GetRequiredService<IConfigureOptions<HttpClientFactoryOptions>>();
        _factory = provider.GetService<IHttpClientFactory>();
    }

    public HttpClient GetHttpClient(HttpClientCacheKey key, Func<HttpClientHandler, HttpClient> createHttpClient)
    {
        _configuration.Register(key);

        return _factory.CreateClient(key.AsString);
    }

    public bool TryRemoveHttpClient(HttpClientCacheKey key, bool force = false)
    {
        return false;
    }

    private sealed class RavenDynamicHttpClientFactoryConfiguration : IConfigureNamedOptions<HttpClientFactoryOptions>
    {
        private readonly object _locker = new object();

        private FrozenDictionary<string, HttpClientCacheKey> _registeredConfigurations = FrozenDictionary<string, HttpClientCacheKey>.Empty;

        public void Register(HttpClientCacheKey key)
        {
            if (_registeredConfigurations.ContainsKey(key.AsString))
                return;

            lock (_locker)
            {
                _registeredConfigurations = new Dictionary<string, HttpClientCacheKey>(_registeredConfigurations)
                {
                    { key.AsString, key }
                }.ToFrozenDictionary(StringComparer.Ordinal);
            }
        }

        public void Configure(HttpClientFactoryOptions options)
        {
            throw new NotSupportedException();
        }

        public void Configure(string name, HttpClientFactoryOptions options)
        {
            if (_registeredConfigurations.TryGetValue(name, out var key) == false)
                throw new InvalidOperationException($"Could not retrieve configuration for '{name}' http client.");

            options.HttpClientActions.Add(DefaultRavenHttpClientFactory.ConfigureHttpClient);

            options.HttpMessageHandlerBuilderActions.Add(builder =>
            {
                var h = builder.PrimaryHandler;
                if (h is not HttpClientHandler httpMessageHandler)
                    return;

                DefaultRavenHttpClientFactory.ConfigureHttpMessageHandler(httpMessageHandler, key.Certificate, setSslProtocols: true, key.UseCompression, key.HasExplicitlySetCompressionUsage, key.PooledConnectionLifetime, key.PooledConnectionIdleTimeout);
            });
        }
    }
}
