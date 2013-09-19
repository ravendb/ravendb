using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Server.WebApi.Handlers
{
	public class GZipToJsonHandler : DelegatingHandler
	{
		protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
															   CancellationToken cancellationToken)
		{
			// Handle only if content type is 'application/gzip'
			if (request.Content.Headers.ContentEncoding == null ||
				request.Content.Headers.ContentEncoding.Contains("gzip") == false)
			{
				return base.SendAsync(request, cancellationToken);
			}

			// Read in the input stream, then decompress in to the outputstream.
			// Doing this asynronously, but not really required at this point
			// since we end up waiting on it right after this.
			Stream outputStream = new MemoryStream();
			Task task = request.Content.ReadAsStreamAsync().ContinueWith(t =>
			{
				Stream inputStream = t.Result;
				var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);

				gzipStream.CopyTo(outputStream);
				gzipStream.Dispose();

				outputStream.Seek(0, SeekOrigin.Begin);
			}, cancellationToken);

			// Wait for inputstream and decompression to complete. Would be nice
			// to not block here and work async when ready instead, but I couldn't 
			// figure out how to do it in context of a DelegatingHandler.
			task.Wait();

			// This next section is the key...

			// Save the original content
			HttpContent origContent = request.Content;

			// Replace request content with the newly decompressed stream
			request.Content = new StreamContent(outputStream);

			// Copy all headers from original content in to new one
			foreach (var header in origContent.Headers)
			{
				request.Content.Headers.Add(header.Key, header.Value);
			}

			// Replace the original content-type with content type
			// of decompressed data. In our case, we can assume application/json. A
			// more generic and reuseable handler would need some other 
			// way to differentiate the decompressed content type.
			request.Content.Headers.Remove("Content-Type");
			request.Content.Headers.Add("Content-Type", "application/json");

			return base.SendAsync(request, cancellationToken);
		}
	}
}
