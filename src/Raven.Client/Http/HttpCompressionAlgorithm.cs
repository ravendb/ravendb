using System;

namespace Raven.Client.Http;

public enum HttpCompressionAlgorithm
{
    Gzip,
#if FEATURE_BROTLI_SUPPORT
    Brotli
#endif
}

internal static class HttpCompressionAlgorithmExtensions
{
    internal static string GetContentEncoding(this HttpCompressionAlgorithm compressionAlgorithm)
    {
        switch (compressionAlgorithm)
        {
            case HttpCompressionAlgorithm.Gzip:
                return Constants.Headers.Encodings.Gzip;
#if FEATURE_BROTLI_SUPPORT
            case HttpCompressionAlgorithm.Brotli:
                return Constants.Headers.Encodings.Brotli;
#endif
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
        }
    }
}
