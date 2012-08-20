//-----------------------------------------------------------------------
// <copyright file="IHttpRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpRequest
	{
		bool IsLocal { get; }
		NameValueCollection Headers { get;  }
		Stream InputStream { get; }
		NameValueCollection QueryString { get; }
		string HttpMethod { get; }
		Uri Url { get; set; }
		string RawUrl { get; set; }
	}
}
