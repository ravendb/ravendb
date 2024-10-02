using System;
using System.IO;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.Extensions.Options;
using Raven.Client;
using Sparrow.Utils;

namespace Raven.Server.Web.ResponseCompression
{
    public sealed class ZstdCompressionProvider : ICompressionProvider
    {
        public ZstdCompressionProvider(IOptions<ZstdCompressionProviderOptions> options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            Options = options.Value;
        }

        private ZstdCompressionProviderOptions Options { get; }

        public Stream CreateStream(Stream outputStream)
        {
            return ZstdStream.Compress(outputStream, Options.Level);
        }

        public string EncodingName => Constants.Headers.Encodings.Zstd;
        public bool SupportsFlush => true;
    }
}
