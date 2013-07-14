//-----------------------------------------------------------------------
// <copyright file="IHttpResponse.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;

namespace Raven.Database.Server.Abstractions
{
	public interface IHttpResponse
	{
		string RedirectionPrefix { get; set; }
		void AddHeader(string name, string value);
		Stream OutputStream { get; }
		long ContentLength64 { get; set; }
		int StatusCode { get; set; }
		string StatusDescription { get; set; }
		string ContentType { get; set; }
		bool BufferOutput { get; }
		void Redirect(string url);
		void Close();
		void SetPublicCacheability();
		void WriteFile(string path);
		NameValueCollection GetHeaders();
		IDisposable Streaming();

		Task WriteAsync(string data);
		void SetCookie(string name, string val);
	}
}
