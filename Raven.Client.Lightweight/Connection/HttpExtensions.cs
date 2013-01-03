// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Data;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#elif NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif

namespace Raven.Client.Connection
{
	public static class HttpExtensions
	{
		public static Guid GetEtagHeader(this HttpWebResponse response)
		{
#if SILVERLIGHT || NETFX_CORE
			return EtagHeaderToGuid(response.Headers["ETag"]);
#else
			return EtagHeaderToGuid(response.GetResponseHeader("ETag"));
#endif
		}

		public static Guid GetEtagHeader(this GetResponse response)
		{
			return EtagHeaderToGuid(response.Headers["ETag"]);
		}


		public static Guid GetEtagHeader(this HttpJsonRequest request)
		{
			return EtagHeaderToGuid(request.ResponseHeaders["ETag"]);
		}

		internal static Guid EtagHeaderToGuid(string responseHeader)
		{
			if (string.IsNullOrEmpty(responseHeader))
				throw new InvalidOperationException("Response didn't had an ETag header");

			if (responseHeader[0] == '\"')
				return new Guid(responseHeader.Substring(1, responseHeader.Length - 2));

			return new Guid(responseHeader);
		}
	}
}