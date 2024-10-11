using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Util;

namespace Raven.Client.Http;

internal sealed class DefaultRavenHttpClientFactory : IRavenHttpClientFactory
{
    private static readonly TimeSpan MinHttpClientLifetime = TimeSpan.FromSeconds(5);

    private static readonly ConcurrentDictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>> GlobalConcurrentHttpClientCache = new();

    private static Dictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>> GlobalHttpClientCache = new();

    public static readonly DefaultRavenHttpClientFactory Instance = new();

    private DefaultRavenHttpClientFactory()
    {
    }

    public HttpClient GetHttpClient(HttpClientCacheKey key, Func<HttpClientHandler, HttpClient> createHttpClient)
    {
        if (GlobalHttpClientCache.TryGetValue(key, out var value))
            return value.Value.HttpClient;

        value = GlobalConcurrentHttpClientCache.GetOrAdd(key, cacheKey => new Lazy<HttpClientCacheItem>(() => new HttpClientCacheItem(CreateClient(cacheKey, createHttpClient), SystemTime.UtcNow)));

        GlobalHttpClientCache = new Dictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>>(GlobalConcurrentHttpClientCache);

        return value.Value.HttpClient;
    }

    public bool TryRemoveHttpClient(HttpClientCacheKey key, bool force = false)
    {
        if (GlobalConcurrentHttpClientCache.TryGetValue(key, out var client) &&
            ((client.IsValueCreated &&
              SystemTime.UtcNow - client.Value.CreatedAt > MinHttpClientLifetime) || force))
        {
            if (GlobalConcurrentHttpClientCache.TryRemove(key, out _))
                GlobalHttpClientCache = new Dictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>>(GlobalConcurrentHttpClientCache);

            return true;
        }

        return false;
    }

    public void Clear()
    {
        GlobalConcurrentHttpClientCache.Clear();
        GlobalHttpClientCache = new Dictionary<HttpClientCacheKey, Lazy<HttpClientCacheItem>>();
    }

    private static HttpClient CreateClient(HttpClientCacheKey key, Func<HttpClientHandler, HttpClient> createHttpClient)
    {
        var httpMessageHandler = CreateHttpMessageHandler(key.Certificate,
            setSslProtocols: true,
            useCompression: key.UseCompression,
            hasExplicitlySetCompressionUsage: key.HasExplicitlySetCompressionUsage,
            key.PooledConnectionLifetime,
            key.PooledConnectionIdleTimeout
        );

        var httpClient = createHttpClient(httpMessageHandler);

        ConfigureHttpClient(httpClient);

        return httpClient;
    }

    internal static void ConfigureHttpClient(HttpClient httpClient)
    {
        httpClient.Timeout = RequestExecutor.GlobalHttpClientTimeout;
    }

    internal static HttpClientHandler CreateHttpMessageHandler(X509Certificate2 certificate, bool setSslProtocols, bool useCompression, bool hasExplicitlySetCompressionUsage = false, TimeSpan? pooledConnectionLifetime = null, TimeSpan? pooledConnectionIdleTimeout = null)
    {
        var httpMessageHandler = new HttpClientHandler();

        ConfigureHttpMessageHandler(httpMessageHandler, certificate, setSslProtocols, useCompression, hasExplicitlySetCompressionUsage, pooledConnectionLifetime, pooledConnectionIdleTimeout);

        return httpMessageHandler;
    }

    internal static void ConfigureHttpMessageHandler(HttpClientHandler httpMessageHandler, X509Certificate2 certificate, bool setSslProtocols, bool useCompression, bool hasExplicitlySetCompressionUsage = false, TimeSpan? pooledConnectionLifetime = null, TimeSpan? pooledConnectionIdleTimeout = null)
    {
        try
        {
            httpMessageHandler.MaxConnectionsPerServer = RequestExecutor.DefaultConnectionLimit;
        }
        catch (NotImplementedException)
        {
            // ignored
        }

        HttpClientHandlerHelper.Configure(httpMessageHandler, pooledConnectionLifetime, pooledConnectionIdleTimeout);

        if (httpMessageHandler.SupportsAutomaticDecompression)
        {
            httpMessageHandler.AutomaticDecompression =
                useCompression ?
                    DecompressionMethods.GZip | DecompressionMethods.Deflate
                    : DecompressionMethods.None;
        }
        else if (useCompression && hasExplicitlySetCompressionUsage)
        {
            throw new NotSupportedException("HttpClient implementation for the current platform does not support request compression.");
        }

        if (RequestExecutor.ServerCertificateCustomValidationCallbackRegistrationException == null)
            httpMessageHandler.ServerCertificateCustomValidationCallback += RequestExecutor.OnServerCertificateCustomValidationCallback;

        if (certificate != null)
        {
            if (httpMessageHandler.ClientCertificates == null)
                throw new NotSupportedException($"{typeof(HttpClientHandler)} does not support {nameof(httpMessageHandler.ClientCertificates)}. Setting the UseNativeHttpHandler property in project settings to false may solve the issue.");

            httpMessageHandler.ClientCertificates.Add(certificate);
            try
            {
                if (setSslProtocols)
                    httpMessageHandler.SslProtocols = TcpUtils.SupportedSslProtocols;
            }
            catch (PlatformNotSupportedException)
            {
                // The user can set the following manually:
                // ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            }

            ValidateClientKeyUsages(certificate);
        }
    }

    private static void ValidateClientKeyUsages(X509Certificate2 certificate)
    {
        var supported = false;
        foreach (var extension in certificate.Extensions)
        {
            if (extension.Oid.Value != "2.5.29.37") //Enhanced Key Usage extension
                continue;

            if (!(extension is X509EnhancedKeyUsageExtension kue))
                continue;

            foreach (var eku in kue.EnhancedKeyUsages)
            {
                if (eku.Value != "1.3.6.1.5.5.7.3.2")
                    continue;

                supported = true;
                break;
            }

            if (supported)
                break;
        }

        if (supported == false)
            throw new InvalidOperationException("Client certificate " + certificate.FriendlyName + " must be defined with the following 'Enhanced Key Usage': Client Authentication (Oid 1.3.6.1.5.5.7.3.2)");
    }

    private sealed class HttpClientCacheItem
    {
        public HttpClientCacheItem(HttpClient httpClient, DateTime createdAt)
        {
            HttpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            CreatedAt = createdAt;
        }

        public readonly HttpClient HttpClient;

        public readonly DateTime CreatedAt;
    }
}
