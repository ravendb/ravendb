using System.IO.Compression;
using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
    /// <summary>
    /// Options for the ZstdCompressionProvider
    /// </summary>
    public sealed class ZstdCompressionProviderOptions : IOptions<ZstdCompressionProviderOptions>
    {
        public CompressionLevel Level { get; set; } = CompressionLevel.Fastest;

        /// <inheritdoc />
        ZstdCompressionProviderOptions IOptions<ZstdCompressionProviderOptions>.Value => this;
    }
}
