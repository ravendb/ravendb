namespace Raven.Client.Http;

public enum HttpCompressionAlgorithm
{
    Gzip,
#if NET6_0_OR_GREATER
    Brotli
#endif
}
