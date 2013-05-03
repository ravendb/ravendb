//-----------------------------------------------------------------------
// <copyright file="HttpRequestAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Web;

namespace Raven.Database.Server.Abstractions
{
	public class HttpRequestAdapter : IHttpRequest
	{
		private readonly HttpRequest request;
	    private readonly NameValueCollection queryString;

	    public HttpRequestAdapter(HttpRequest request)
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

		public Stream GetBufferLessInputStream()
		{
			return request.GetBufferlessInputStream();
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

		public Stream InputStream
		{
			get { return request.InputStream; }
		}

		public long ContentLength
		{
			get { return request.ContentLength; }
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
	}
}
