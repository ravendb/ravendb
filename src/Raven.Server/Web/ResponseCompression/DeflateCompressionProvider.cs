using System;
using System.IO;
using System.IO.Compression;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.Extensions.Options;

namespace Raven.Server.Web.ResponseCompression
{
    public class DeflateCompressionProvider : ICompressionProvider
    {
        public DeflateCompressionProvider(IOptions<DeflateCompressionProviderOptions> options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            Options = options.Value;
        }

        private DeflateCompressionProviderOptions Options { get; }

        public Stream CreateStream(Stream outputStream)
        {
            return new DeflateStream(outputStream, Options.Level, true);
        }

        public string EncodingName => "deflate";
        public bool SupportsFlush => false;
    }
}
