//-----------------------------------------------------------------------
// <copyright file="HttpResponseAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Database.Server.Abstractions
{
	public class HttpResponseAdapter : IHttpResponse
	{
		private readonly HttpResponse response;
		private bool ignoreHeadersFromNowOn;
		public HttpResponseAdapter(HttpResponse response)
		{
			this.response = response;
		}

		public string RedirectionPrefix { get; set; }

		public void AddHeader(string name, string value)
		{
			if (ignoreHeadersFromNowOn)
				return;

			if (name == "ETag" && string.IsNullOrEmpty(response.CacheControl))
				response.AddHeader("Expires", "Sat, 01 Jan 2000 00:00:00 GMT");
			
			response.AddHeader(name, value);
		}

		public Stream OutputStream
		{
			get
			{
				return response.OutputStream;
			}
		}

		public long ContentLength64
		{
			get { return -1; }
			set { }
		}

		public int StatusCode
		{
			get { return response.StatusCode; }
			set
			{
				response.TrySkipIisCustomErrors = true;
				response.StatusCode = value;
			}
		}

		public string StatusDescription
		{
			get { return response.StatusDescription; }
			set { response.StatusDescription = value; }
		}

		public bool BufferOutput
		{
			get { return response.BufferOutput; }
		}

		public void Redirect(string url)
		{
			response.Redirect(RedirectionPrefix + url, false);
		}

		public void Close()
		{
			response.Close();
		}

		public void SetPublicCacheability()
		{
			response.Cache.SetCacheability(HttpCacheability.Public);
		}

		public void WriteFile(string path)
		{
			response.WriteFile(path);
		}

		public NameValueCollection GetHeaders()
		{
			return response.Headers;
		}

		public IDisposable Streaming()
		{
			response.BufferOutput = false;
			return new DisposableAction(() =>
			{
				response.BufferOutput = true;
				ignoreHeadersFromNowOn = true;
			});
		}

		public string ContentType
		{
			get { return response.ContentType; }
			set { response.ContentType = value; }
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
			response.SetCookie(new HttpCookie(name,val)
			{
				HttpOnly = true,
				Expires = SystemTime.UtcNow.AddHours(1),
				Path = "/"
			});
		}
	}
}
