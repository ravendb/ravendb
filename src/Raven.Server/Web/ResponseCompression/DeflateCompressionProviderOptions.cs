using System.IO.Compression;
using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
    /// <summary>
    /// Options for the DeflateCompressionProvider
    /// </summary>
    public class DeflateCompressionProviderOptions : IOptions<DeflateCompressionProviderOptions>
    {
        /// <summary>
        /// What level of compression to use for the stream. The default is Fastest.
        /// </summary>
        public CompressionLevel Level { get; set; } = CompressionLevel.Fastest;

        /// <inheritdoc />
        DeflateCompressionProviderOptions IOptions<DeflateCompressionProviderOptions>.Value => this;
    }
}
