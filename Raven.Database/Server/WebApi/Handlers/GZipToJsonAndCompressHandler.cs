using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Database.Server.Connections;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.WebApi.Handlers
{
    public class GZipToJsonAndCompressHandler : DelegatingHandler
    {
        [ThreadStatic]
        private static byte[] buffer;

        private void CopyBetween(Stream source, Stream destination)
        {
            if (buffer == null)
            {
                buffer = new byte[4*1024];
            }


            int read;
            while ((read = source.Read(buffer, 0, buffer.Length)) != 0)
                destination.Write(buffer, 0, read);
        }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                               CancellationToken cancellationToken)
        {
            HttpResponseMessage response;
            // Handle only if content type is 'application/gzip'
            var contentEncoding = request.Content.Headers.ContentEncoding.FirstOrDefault();
            if (contentEncoding == null ||
                contentEncoding.Contains("gzip") == false)
            {
                response = await base.SendAsync(request, cancellationToken);
                return Compress(response);
            }

            // Read in the input stream, then decompress in to the output stream.
            // Doing this asynchronously, but not really required at this point
            // since we end up waiting on it right after this.
            Stream outputStream = new MemoryStream();
            await request.Content.ReadAsStreamAsync().ContinueWith(t =>
            {
                Stream inputStream = t.Result;
                var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);

                CopyBetween(gzipStream, outputStream);
                
                gzipStream.Dispose();

                outputStream.Seek(0, SeekOrigin.Begin);
            }, cancellationToken);

            // This next section is the key...

            // Save the original content
            HttpContent origContent = request.Content;

            // Replace request content with the newly decompressed stream
            request.Content = new StreamContent(outputStream);

            // Copy all headers from original content in to new one
            foreach (var header in origContent.Headers)
            {
                foreach (var val in header.Value)
                {
                    request.Content.Headers.Add(header.Key, val);
                }
            }

            response = await base.SendAsync(request, cancellationToken);
            return Compress(response);
        }

        public HttpResponseMessage Compress(HttpResponseMessage response)
        {
            if (response.RequestMessage != null && 
                response.RequestMessage.Headers.AcceptEncoding != null && 
                response.RequestMessage.Headers.AcceptEncoding.Count != 0 && 
                response.Content != null &&
                response.Content is IEventsTransport == false &&
                response.Content is CompressedContent == false &&
                response.Content is CompressedStreamContent == false &&
                response.Content is CompressedStringContent == false)
            {
                string encodingType = response.RequestMessage.Headers.AcceptEncoding.First().Value;
                if (encodingType == "gzip" || encodingType == "deflate")
                {
                    response.Content = new CompressedContent(response.Content, encodingType);
                }
            }

            return response;
        }
    }
}
