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
			this.queryString = System.Web.HttpUtility.ParseQueryString(Uri.UnescapeDataString(request.Url.Query));
	       
		}

		public bool IsLocal
		{
			get { return request.IsLocal; }
		}
		public NameValueCollection Headers
		{
			get { return request.Headers; }
		}

		public Stream InputStream
		{
			get { return request.InputStream; }
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
