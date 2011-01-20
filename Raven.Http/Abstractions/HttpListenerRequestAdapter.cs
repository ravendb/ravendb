//-----------------------------------------------------------------------
// <copyright file="HttpListenerRequestAdapter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace Raven.Http.Abstractions
{
	public class HttpListenerRequestAdapter : IHttpRequest
	{
		private readonly HttpListenerRequest request;

	    private NameValueCollection queryString;

	    public HttpListenerRequestAdapter(HttpListenerRequest request)
		{
			this.request = request;
		    this.queryString = System.Web.HttpUtility.ParseQueryString(Uri.UnescapeDataString(request.Url.Query));
	        Url = this.request.Url;
	        RawUrl = this.request.RawUrl;
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
