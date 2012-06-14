// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Data;
using System.Linq;

#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif

namespace Raven.Client.Connection
{
	public static class HttpExtensions
	{
		public static Guid GetEtagHeader(this HttpWebResponse response)
		{
#if SILVERLIGHT
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
#if SILVERLIGHT
			return EtagHeaderToGuid(request.ResponseHeaders["ETag"].FirstOrDefault());
#else
			return EtagHeaderToGuid(request.ResponseHeaders["ETag"]);
#endif
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