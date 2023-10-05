using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
#if FEATURE_ZSTD_SUPPORT
    /// <summary>
    /// Options for the ZstdCompressionProvider
    /// </summary>
    public sealed class ZstdCompressionProviderOptions : IOptions<ZstdCompressionProviderOptions>
    {
        /// <inheritdoc />
        ZstdCompressionProviderOptions IOptions<ZstdCompressionProviderOptions>.Value => this;
    }
#endif
}
