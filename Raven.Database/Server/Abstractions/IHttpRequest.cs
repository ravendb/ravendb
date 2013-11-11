//-----------------------------------------------------------------------
// <copyright file="IHttpRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Web;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpRequest
	{
		bool IsLocal { get; }
		NameValueCollection Headers { get;  }
		Stream InputStream { get; }
		long ContentLength { get; }
		NameValueCollection QueryString { get; }
		string HttpMethod { get; }
		Uri Url { get; set; }
		string RawUrl { get; set; }
		Stream GetBufferLessInputStream();
		bool HasCookie(string name);
		string GetCookie(string name);
	}

	internal class HttpRequestHelper
	{
		public static NameValueCollection ParseQueryStringWithLegacySupport(string ravenClientVersion, string query)
		{
			if (ravenClientVersion == null || ravenClientVersion.StartsWith("1.0") || ravenClientVersion.StartsWith("2.0"))
			{
				query = Uri.UnescapeDataString(query);
			}
			return HttpUtility.ParseQueryString(query);
		}
	}
}
