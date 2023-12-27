using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Raven.Client.Http;

public enum HttpCompressionAlgorithm
{
    Gzip,
#if FEATURE_BROTLI_SUPPORT
    Brotli,
#endif
#if FEATURE_ZSTD_SUPPORT
    Zstd
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
#if FEATURE_ZSTD_SUPPORT
            case HttpCompressionAlgorithm.Zstd:
                return Constants.Headers.Encodings.Zstd;
#endif
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
        }
    }

    internal static async Task<string> ReadAsStringWithZstdSupportAsync(this HttpContent httpContent, CancellationToken cancellationToken = default)
    {
#if FEATURE_ZSTD_SUPPORT
        if (IsContentEncodingZstd(httpContent))
        {
            await using (var contentStream = await httpContent.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
            await using (var zstdStream = ZstdStream.Decompress(contentStream))
            using (var streamReader = new StreamReader(zstdStream))
            {
#if NET6_0
                return await streamReader.ReadToEndAsync().ConfigureAwait(false);
#else
                return await streamReader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
#endif
            }
        }
#endif

#if NET6_0_OR_GREATER
        return await httpContent.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
#else
        return await httpContent.ReadAsStringAsync().ConfigureAwait(false);
#endif
    }

    internal static async Task<Stream> ReadAsStreamWithZstdSupportAsync(this HttpContent httpContent)
    {
        var contentStream = await httpContent.ReadAsStreamAsync().ConfigureAwait(false);
        var contentStreamType = contentStream.GetType();
#if FEATURE_BROTLI_SUPPORT
        if (contentStreamType == typeof(BrotliStream))
            return contentStream;
#endif
        if (contentStreamType == typeof(GZipStream))
            return contentStream;

#if FEATURE_ZSTD_SUPPORT
        if (IsContentEncodingZstd(httpContent))
            return ZstdStream.Decompress(contentStream);

        return contentStream;
#else
        return contentStream;
#endif
    }

#if FEATURE_ZSTD_SUPPORT
    internal static bool IsContentEncodingZstd(HttpContent httpContent)
    {
        if (httpContent.Headers.TryGetValues(Constants.Headers.ContentEncoding, out var values) == false)
            return false;

        foreach (var value in values)
        {
            if (value == Constants.Headers.Encodings.Zstd)
                return true;
        }

        return false;
    }
#endif
}
