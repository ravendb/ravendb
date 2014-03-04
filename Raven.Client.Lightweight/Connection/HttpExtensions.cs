// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Net;
using System.Net.Http;
using Raven.Abstractions.Data;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif

namespace Raven.Client.Connection
{
    public static class HttpExtensions
    {
        public static Etag GetEtagHeader(this NameValueCollection headers)
        {
            return EtagHeaderToEtag(headers["ETag"]);
        }

        public static Etag GetEtagHeader(this HttpWebResponse response)
        {
#if NETFX_CORE
			return EtagHeaderToEtag(response.Headers["ETag"]);
#else
            return EtagHeaderToEtag(response.GetResponseHeader("ETag"));
#endif
        }

        public static Etag GetEtagHeader(this HttpResponseMessage response)
        {
            return EtagHeaderToEtag(response.Headers.ETag.Tag);
        }

        public static Etag GetEtagHeader(this GetResponse response)
        {
            return EtagHeaderToEtag(response.Headers["ETag"]);
        }

        public static Etag GetEtagHeader(this HttpJsonRequest request)
        {
            return EtagHeaderToEtag(request.ResponseHeaders["ETag"]);
        }

        internal static Etag EtagHeaderToEtag(string responseHeader)
        {
            if (string.IsNullOrEmpty(responseHeader))
                throw new InvalidOperationException("Response didn't had an ETag header");

            if (responseHeader[0] == '\"')
                return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

            return Etag.Parse(responseHeader);
        }
    }
}
