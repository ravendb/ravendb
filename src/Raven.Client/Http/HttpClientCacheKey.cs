using System;
using System.Security.Cryptography.X509Certificates;

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

    public readonly string AsString;

    internal HttpClientCacheKey(X509Certificate2 certificate, bool useHttpDecompression, bool hasExplicitlySetDecompressionUsage, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout, TimeSpan globalHttpClientTimeout, Type httpClientType)
    {
        Certificate = certificate;
        _certificateThumbprint = certificate?.Thumbprint ?? string.Empty;
        UseHttpDecompression = useHttpDecompression;
        HasExplicitlySetDecompressionUsage = hasExplicitlySetDecompressionUsage;
        PooledConnectionLifetime = pooledConnectionLifetime;
        PooledConnectionIdleTimeout = pooledConnectionIdleTimeout;
        GlobalHttpClientTimeout = globalHttpClientTimeout;
        _httpClientType = httpClientType;

        AsString = $"{_certificateThumbprint}|{UseHttpDecompression}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{globalHttpClientTimeout.TotalMilliseconds}|{httpClientType.Name}";
    }

    private bool Equals(HttpClientCacheKey other)
    {
        return _certificateThumbprint == other._certificateThumbprint
               && UseHttpDecompression == other.UseHttpDecompression
               && Nullable.Equals(PooledConnectionLifetime, other.PooledConnectionLifetime)
               && Nullable.Equals(PooledConnectionIdleTimeout, other.PooledConnectionIdleTimeout)
               //&& Nullable.Equals(GlobalHttpClientTimeout, other.GlobalHttpClientTimeout) not checking this because we can have same handler with different timeouts in HttpClient
               && _httpClientType == other._httpClientType;
    }

    public override bool Equals(object obj)
    {
        return obj is HttpClientCacheKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(_certificateThumbprint, UseHttpDecompression, PooledConnectionLifetime, PooledConnectionIdleTimeout, _httpClientType);
    }
}
