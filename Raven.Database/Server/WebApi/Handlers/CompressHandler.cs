using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Server.WebApi.Handlers
{
	public class CompressHandler : DelegatingHandler
	{
		protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
		{
			return base.SendAsync(request, cancellationToken).ContinueWith(responseToCompleteTask =>
			{
				HttpResponseMessage response = responseToCompleteTask.Result;

				if (response.RequestMessage != null && response.RequestMessage.Headers.AcceptEncoding != null && response.RequestMessage.Headers.AcceptEncoding.Count != 0)
				{
					string encodingType = response.RequestMessage.Headers.AcceptEncoding.First().Value;

					response.Content = new CompressedContent(response.Content, encodingType);
				}

				return response;
			},
				TaskContinuationOptions.OnlyOnRanToCompletion);
		}
	}

	public class CompressedContent : HttpContent
	{
		private HttpContent originalContent;
		private string encodingType;

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

		protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			Stream compressedStream = null;

			if (encodingType == "gzip")
			{
				compressedStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
			}
			else if (encodingType == "deflate")
			{
				compressedStream = new DeflateStream(stream, CompressionMode.Compress, leaveOpen: true);
			}

			return originalContent.CopyToAsync(compressedStream).ContinueWith(tsk =>
			{
				if (compressedStream != null)
				{
					compressedStream.Dispose();
				}
			});
		}
	}
}