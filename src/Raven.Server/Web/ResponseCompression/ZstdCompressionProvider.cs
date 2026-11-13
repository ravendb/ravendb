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

        internal ICompressionProvider WithSizeHint(long contentLength) => new SizeHintCompressionProvider(this, contentLength);

        private sealed class SizeHintCompressionProvider(ZstdCompressionProvider parent, long contentLength) : ICompressionProvider
        {
            public Stream CreateStream(Stream outputStream) => ZstdStream.CompressWithSizeHint(outputStream, parent.Options.Level, contentLength);

            public string EncodingName => parent.EncodingName;

            public bool SupportsFlush => parent.SupportsFlush;
        }

        public string EncodingName => Constants.Headers.Encodings.Zstd;
        public bool SupportsFlush => true;
    }
}
