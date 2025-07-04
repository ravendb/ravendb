using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Util;

namespace Raven.Client.Http;

internal readonly struct HttpClientCacheKey
{
    private readonly string _certificateThumbprint;
    internal readonly X509Certificate2 Certificate;
    internal readonly bool UseHttpDecompression;
    internal readonly bool HasExplicitlySetDecompressionUsage;
    internal readonly TimeSpan? PooledConnectionLifetime;
    internal readonly TimeSpan? PooledConnectionIdleTimeout;
    internal readonly TimeSpan GlobalHttpClientTimeout;
    private readonly Type _httpClientType;
    private readonly string _apiKey;
    private readonly string _baseUri;

    internal readonly Action<HttpMessageHandler> ConfigureHttpMessageHandler;

    public readonly string AsString;

    internal static HttpClientCacheKey CreateHttpWithApiKey(bool useHttpDecompression, bool hasExplicitlySetDecompressionUsage, TimeSpan? pooledConnectionLifetime,
        TimeSpan? pooledConnectionIdleTimeout, TimeSpan globalHttpClientTimeout, string baseUri, string apiKey, Action<HttpMessageHandler> configureHttpMessageHandler)
    {
        ValidationMethods.AssertNotNullOrEmpty(baseUri, nameof(baseUri));
        // apiKey can be null (or empty)

        return new HttpClientCacheKey(certificate: null, useHttpDecompression, hasExplicitlySetDecompressionUsage, pooledConnectionLifetime, pooledConnectionIdleTimeout,
            globalHttpClientTimeout,
            httpClientType: null, baseUri, apiKey, configureHttpMessageHandler);
    }

    internal static HttpClientCacheKey Create(X509Certificate2 certificate, bool useHttpDecompression, bool hasExplicitlySetDecompressionUsage, TimeSpan? pooledConnectionLifetime,
        TimeSpan? pooledConnectionIdleTimeout, TimeSpan globalHttpClientTimeout, Type httpClientType, Action<HttpMessageHandler> configureHttpMessageHandler)
    {
        ValidationMethods.AssertNotNullOrEmpty(httpClientType, nameof(httpClientType));

        return new HttpClientCacheKey(certificate, useHttpDecompression, hasExplicitlySetDecompressionUsage, pooledConnectionLifetime, pooledConnectionIdleTimeout,
            globalHttpClientTimeout,
            httpClientType, baseUri: null, apiKey: null, configureHttpMessageHandler);
    }

    private HttpClientCacheKey(X509Certificate2 certificate, bool useHttpDecompression, bool hasExplicitlySetDecompressionUsage, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout, TimeSpan globalHttpClientTimeout, Type httpClientType, string baseUri, string apiKey, Action<HttpMessageHandler> configureHttpMessageHandler)
    {
        Certificate = certificate;
        _certificateThumbprint = certificate?.Thumbprint ?? string.Empty;
        UseHttpDecompression = useHttpDecompression;
        HasExplicitlySetDecompressionUsage = hasExplicitlySetDecompressionUsage;
        PooledConnectionLifetime = pooledConnectionLifetime;
        PooledConnectionIdleTimeout = pooledConnectionIdleTimeout;
        GlobalHttpClientTimeout = globalHttpClientTimeout;
        _httpClientType = httpClientType;
        _baseUri = baseUri;
        _apiKey = apiKey;
        ConfigureHttpMessageHandler = configureHttpMessageHandler;

        AsString = $"{_certificateThumbprint}|{UseHttpDecompression}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{globalHttpClientTimeout.TotalMilliseconds}|{httpClientType?.Name}|{configureHttpMessageHandler?.Method.Name}|{baseUri}|{apiKey}";
    }

    private bool Equals(HttpClientCacheKey other)
    {
        return _certificateThumbprint == other._certificateThumbprint
               && UseHttpDecompression == other.UseHttpDecompression
               && Nullable.Equals(PooledConnectionLifetime, other.PooledConnectionLifetime)
               && Nullable.Equals(PooledConnectionIdleTimeout, other.PooledConnectionIdleTimeout)
               //&& Nullable.Equals(GlobalHttpClientTimeout, other.GlobalHttpClientTimeout) not checking this because we can have same handler with different timeouts in HttpClient
               && _httpClientType == other._httpClientType
               && _apiKey == other._apiKey
               && _baseUri == other._baseUri;
    }

    public override bool Equals(object obj)
    {
        return obj is HttpClientCacheKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(_certificateThumbprint, UseHttpDecompression, PooledConnectionLifetime, PooledConnectionIdleTimeout, _httpClientType, _baseUri, _apiKey);
    }
}
