using Raven.Abstractions.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Connection
{
    public class CompressedContent : HttpContent
    {
        private readonly HttpContent originalContent;
        private readonly string encodingType;

        public HttpContent OriginalContent
        {
            get { return originalContent; }
        }

        public CompressedContent(HttpContent content, string encodingType)
        {
            if (content == null)
            {
                throw new ArgumentNullException("content");
            }

            if (encodingType == null)
            {
                throw new ArgumentNullException("encodingType");
            }

            originalContent = content;
            this.encodingType = encodingType.ToLowerInvariant();

            if (this.encodingType != "gzip" && this.encodingType != "deflate")
            {
                throw new InvalidOperationException(string.Format("Encoding '{0}' is not supported. Only supports gzip or deflate encoding.", this.encodingType));
            }

            // copy the headers from the original content
            foreach (KeyValuePair<string, IEnumerable<string>> header in originalContent.Headers)
            {
                Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            Headers.ContentEncoding.Add(encodingType);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;

            return false;
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            using (var uncloseableStream = new UndisposableStream(stream))
            using (var bufferedStream = new BufferedStream(uncloseableStream))
            {
                var streamToUse = bufferedStream;
                Stream compressedStream = null;

                if (encodingType == "gzip")
                {
                    compressedStream = new GZipStream(streamToUse, CompressionMode.Compress, leaveOpen: true);
                }
                else if (encodingType == "deflate")
                {
                    compressedStream = new DeflateStream(streamToUse, CompressionMode.Compress, leaveOpen: true);
                }
                else throw new InvalidOperationException("This shouldn't happen, ever.");

                await originalContent.CopyToAsync(compressedStream);

                if (compressedStream != null)
                {
                    compressedStream.Dispose();
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (originalContent != null)
                originalContent.Dispose();

            base.Dispose(disposing);
        }
    }
}
