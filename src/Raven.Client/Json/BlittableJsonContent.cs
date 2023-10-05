using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;

namespace Raven.Client.Json
{
    internal sealed class BlittableJsonContent : HttpContent
    {
        private readonly Func<Stream, Task> _asyncTaskWriter;
        private readonly DocumentConventions _conventions;

        public BlittableJsonContent(Func<Stream, Task> writer, DocumentConventions conventions)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            _conventions = conventions;

            if (_conventions.UseHttpCompression)
            {
                switch (_conventions.HttpCompressionAlgorithm)
                {
                    case HttpCompressionAlgorithm.Gzip:
                        Headers.ContentEncoding.Add("gzip");
                        break;
#if NET6_0_OR_GREATER
                    case HttpCompressionAlgorithm.Brotli:
                        Headers.ContentEncoding.Add("br");
                        break;
#endif
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (_conventions.UseHttpCompression == false)
            {
                await _asyncTaskWriter(stream).ConfigureAwait(false);
                return;
            }

            switch (_conventions.HttpCompressionAlgorithm)
            {
                case HttpCompressionAlgorithm.Gzip:
#if NETSTANDARD2_0 || NETCOREAPP2_1
                    using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#else
                    await using (var gzipStream = new GZipStream(stream, CompressionLevel.Fastest, leaveOpen: true))
#endif
                    {
                        await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
                    }
                    break;
#if NET6_0_OR_GREATER
                case HttpCompressionAlgorithm.Brotli:
                    await using (var brotliStream = new BrotliStream(stream, CompressionLevel.Fastest, leaveOpen: true))
                    {
                        await _asyncTaskWriter(brotliStream).ConfigureAwait(false);
                    }
                    break;
#endif
                default:
                    throw new ArgumentOutOfRangeException();
            }


        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
