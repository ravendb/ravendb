//-----------------------------------------------------------------------
// <copyright file="HttpListenerResponseAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Util.Streams;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerResponseAdapter : IHttpResponse, IDisposable
	{
		private ILog log = LogManager.GetCurrentClassLogger();
		private readonly HttpListenerResponse response;

		public HttpListenerResponseAdapter(HttpListenerResponse response, IBufferPool bufferPool)
		{
			StreamsToDispose = new List<Stream>();
			this.response = response;
			OutputStream = new BufferPoolStream(response.OutputStream, bufferPool);
		}

		public string RedirectionPrefix
		{
			get;
			set;
		}

		public void AddHeader(string name, string value)
		{
			if (name == "ETag" && string.IsNullOrEmpty(response.Headers["Cache-Control"]))
				response.AddHeader("Expires", "Sat, 01 Jan 2000 00:00:00 GMT");
			response.AddHeader(name, value);
		}

		public Stream OutputStream { get; set; }

		public long ContentLength64
		{
			get { return response.ContentLength64; }
			set { response.ContentLength64 = value; }
		}

		public int StatusCode
		{
			get { return response.StatusCode; }
			set { response.StatusCode = value; }
		}

		public string StatusDescription
		{
			get { return response.StatusDescription; }
			set { response.StatusDescription = value; }
		}

		public string ContentType
		{
			get { return response.ContentType; }
			set { response.ContentType = value; }
		}

		// for HTTP Listener, we never actually buffer, but we pretend we do
		// so we won't send the headers after we already sent data to the client
		private bool bufferOutput = true;
		public bool BufferOutput
		{
			get { return bufferOutput; }
		}

		public void Redirect(string url)
		{
			response.Redirect(RedirectionPrefix + url);
		}

		public List<Stream> StreamsToDispose { get; set; }

		public void Close()
		{
			var exceptionAggregator = new ExceptionAggregator(log, "Failed to close response", LogLevel.Info);
			exceptionAggregator.Execute(OutputStream.Flush);
			exceptionAggregator.Execute(OutputStream.Dispose);
			if (StreamsToDispose != null)
			{
				foreach (var stream in StreamsToDispose)
				{
					exceptionAggregator.Execute(stream.Flush);
					exceptionAggregator.Execute(stream.Dispose);
				}
			}
			exceptionAggregator.Execute(response.Close);

			exceptionAggregator.ThrowIfNeeded();
		}

		public void WriteFile(string path)
		{
			using (var file = File.OpenRead(path))
			{
				file.CopyTo(OutputStream);
			}
		}

		public NameValueCollection GetHeaders()
		{
			return response.Headers;
		}

		public IDisposable Streaming()
		{
			bufferOutput = false;
			return new DisposableAction(() => bufferOutput = true);
		}

		public Task WriteAsync(string data)
		{
			try
			{
				var bytes = Encoding.UTF8.GetBytes(data);
				return Task.Factory.FromAsync(
					(callback, state) => response.OutputStream.BeginWrite(bytes, 0, bytes.Length, callback, state),
					response.OutputStream.EndWrite,
					null)
					.ContinueWith(task =>
					{
						if (task.IsFaulted)
							return task;
						response.OutputStream.Flush();
						return task;
					})
					.Unwrap();
			}
			catch (Exception e)
			{
				return new CompletedTask(e);
			}
		}

		public void SetCookie(string name, string val)
		{
			response.SetCookie(new Cookie(name, val)
			{
				Expires = SystemTime.UtcNow.AddHours(1),
				HttpOnly = true,
				Path = "/"
			});
		}

		public void SetPublicCacheability()
		{
			response.Headers["Cache-Control"] = "Public";
		}

		public void Dispose()
		{
			if (OutputStream != null)
				OutputStream.Dispose();
		}
	}
}
