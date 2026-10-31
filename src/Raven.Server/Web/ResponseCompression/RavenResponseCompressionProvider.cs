using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
    public sealed class RavenResponseCompressionProvider : ResponseCompressionProvider
    {
        private const int MaxCachedAcceptEncodings = 32;

        private FrozenDictionary<string, ICompressionProvider> _providersByAcceptEncoding = FrozenDictionary<string, ICompressionProvider>.Empty;

        public RavenResponseCompressionProvider(IServiceProvider services, IOptions<ResponseCompressionOptions> options) : base(services, options)
        {
        }

        public override bool ShouldCompressResponse(HttpContext context)
        {
            if (context.Features.Get<SkipResponseCompressionFeature>() != null)
                return false;

            return base.ShouldCompressResponse(context);
        }

        public override ICompressionProvider GetCompressionProvider(HttpContext context)
        {
            var provider = GetCompressionProviderForAcceptEncoding(context);

            // the size of the whole response is known when a single writer produced all of it
            if (provider is ZstdCompressionProvider zstd && context.Features.Get<ResponseContentLengthFeature>() is { } contentLength)
                return zstd.WithSizeHint(contentLength.Length);

            return provider;
        }

        private ICompressionProvider GetCompressionProviderForAcceptEncoding(HttpContext context)
        {
            var acceptEncoding = context.Request.Headers.AcceptEncoding;
            if (acceptEncoding.Count != 1 || acceptEncoding[0] is not { } value)
                return base.GetCompressionProvider(context);

            var providers = _providersByAcceptEncoding;
            if (providers.TryGetValue(value, out var provider))
                return provider;

            provider = base.GetCompressionProvider(context);

            if (providers.Count < MaxCachedAcceptEncodings)
            {
                var updated = new Dictionary<string, ICompressionProvider>(providers, StringComparer.Ordinal) { [value] = provider };
                _providersByAcceptEncoding = updated.ToFrozenDictionary(StringComparer.Ordinal);
            }

            return provider;
        }
    }

    public sealed class ResponseContentLengthFeature(long length)
    {
        public readonly long Length = length;
    }

    public sealed class SkipResponseCompressionFeature
    {
        public static readonly SkipResponseCompressionFeature Instance = new();

        private SkipResponseCompressionFeature()
        {
        }
    }
}
