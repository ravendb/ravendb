namespace Raven.Client.Http;

public enum HttpCompressionAlgorithm
{
    Gzip,
#if FEATURE_BROTLI_SUPPORT
    Brotli
#endif
}
