using System;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Client.Http;

public readonly struct HttpClientCacheKey
{
    private readonly string _certificateThumbprint;
    internal readonly X509Certificate2 Certificate;
    internal readonly bool UseCompression;
    internal readonly bool HasExplicitlySetCompressionUsage;
    internal readonly TimeSpan? PooledConnectionLifetime;
    internal readonly TimeSpan? PooledConnectionIdleTimeout;
    private readonly Type _httpClientType;

    public readonly string AsString;

    internal HttpClientCacheKey(X509Certificate2 certificate, bool useCompression, bool hasExplicitlySetCompressionUsage, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout, Type httpClientType)
    {
        Certificate = certificate;
        _certificateThumbprint = certificate?.Thumbprint ?? string.Empty;
        UseCompression = useCompression;
        HasExplicitlySetCompressionUsage = hasExplicitlySetCompressionUsage;
        PooledConnectionLifetime = pooledConnectionLifetime;
        PooledConnectionIdleTimeout = pooledConnectionIdleTimeout;
        _httpClientType = httpClientType;

        AsString = $"{_certificateThumbprint}|{UseCompression}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{pooledConnectionIdleTimeout?.TotalMilliseconds}|{httpClientType.Name}";
    }

    private bool Equals(HttpClientCacheKey other)
    {
        return _certificateThumbprint == other._certificateThumbprint
               && UseCompression == other.UseCompression
               && Nullable.Equals(PooledConnectionLifetime, other.PooledConnectionLifetime)
               && Nullable.Equals(PooledConnectionIdleTimeout, other.PooledConnectionIdleTimeout)
               && _httpClientType == other._httpClientType;
    }

    public override bool Equals(object obj)
    {
        return obj is HttpClientCacheKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(_certificateThumbprint, UseCompression, PooledConnectionLifetime, PooledConnectionIdleTimeout, _httpClientType);
    }
}
