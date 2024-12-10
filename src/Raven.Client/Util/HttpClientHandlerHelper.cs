using System;
using System.Net.Http;
using System.Reflection;

namespace Raven.Client.Util;

internal static class HttpClientHandlerHelper
{
#if NETCOREAPP3_1_OR_GREATER
    private static readonly FieldInfo GetUnderlyingHandler;
#endif

    static HttpClientHandlerHelper()
    {
#if NETCOREAPP3_1_OR_GREATER
        GetUnderlyingHandler = typeof(HttpClientHandler).GetField("_underlyingHandler", BindingFlags.Instance | BindingFlags.NonPublic) ??
                               typeof(HttpClientHandler).GetField("_socketsHttpHandler", BindingFlags.Instance | BindingFlags.NonPublic);
#if DEBUG
        if (GetUnderlyingHandler == null)
            throw new InvalidOperationException("Could not get underlying handler field from HttpClientHandler.");
#endif
#endif
    }

    public static void Configure(HttpClientHandler handler, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

#if NETCOREAPP3_1_OR_GREATER
        if (GetUnderlyingHandler == null)
            return;

        if (GetUnderlyingHandler.GetValue(handler) is not SocketsHttpHandler underlyingHandler)
        {
            if (pooledConnectionLifetime == null && pooledConnectionIdleTimeout == null)
                return;

            throw new InvalidOperationException("Underlying handler for HttpClientHandler is not SocketsHttpHandler");
        }

        Configure(underlyingHandler, pooledConnectionLifetime, pooledConnectionIdleTimeout);
#else
        if (pooledConnectionLifetime == null && pooledConnectionIdleTimeout == null)
            return;

        throw new InvalidOperationException("Cannot set 'PooledConnectionLifetime' and 'PooledConnectionIdleTimeout' in this framework. Should not happen!");
#endif
    }

#if NETCOREAPP3_1_OR_GREATER
    public static void Configure(SocketsHttpHandler handler, TimeSpan? pooledConnectionLifetime, TimeSpan? pooledConnectionIdleTimeout)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        handler.EnableMultipleHttp2Connections = true;

        if (pooledConnectionLifetime == null && pooledConnectionIdleTimeout == null)
            return;

        if (pooledConnectionLifetime.HasValue)
            handler.PooledConnectionLifetime = pooledConnectionLifetime.Value;

        if (pooledConnectionIdleTimeout.HasValue)
            handler.PooledConnectionIdleTimeout = pooledConnectionIdleTimeout.Value;
    }
#endif
}
