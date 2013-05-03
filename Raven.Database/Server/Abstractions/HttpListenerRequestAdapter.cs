//-----------------------------------------------------------------------
// <copyright file="HttpListenerRequestAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerRequestAdapter : IHttpRequest
	{
		private readonly HttpListenerRequest request;
	    private readonly NameValueCollection queryString;

	    public HttpListenerRequestAdapter(HttpListenerRequest request)
		{
			this.request = request;
		    Url = this.request.Url;
	        RawUrl = this.request.RawUrl;
		    queryString = HttpRequestHelper.ParseQueryStringWithLegacySupport(request.Headers["Raven-Client-Version"], request.Url.Query);
		}

		public bool IsLocal
		{
			get { return request.IsLocal; }
		}
		public NameValueCollection Headers
		{
			get { return request.Headers; }
		}

		private Stream inputStream;
		public Stream InputStream
		{
			get { return inputStream ?? (inputStream = request.InputStream); }
			set { inputStream = value; }
		}

		public long ContentLength
		{
			get { return request.ContentLength64; }
		}

		public NameValueCollection QueryString
		{
			get { return queryString; }
		}

	    public Uri Url { get;  set; }

	    public string HttpMethod
		{
			get { return request.HttpMethod; }
		}

	    public string RawUrl { get;  set; }

		public Stream GetBufferLessInputStream()
		{
			return request.InputStream;
		}

		public bool HasCookie(string name)
		{
			return request.Cookies[name] != null;
		}

		public string GetCookie(string name)
		{
			var cookie = request.Cookies[name];
			if (cookie == null)
			{
				return null;
			}
			return cookie.Value;
		}
	}
}
