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
using Raven.NewClient.Client.Connection.Implementation;

namespace Raven.NewClient.Client.Connection
{
    public static class HttpExtensions
    {
        public static long? GetEtagHeader(this NameValueCollection headers)
        {
            return EtagHeaderToEtag(headers[Constants.MetadataEtagField]);
        }

        public static long? GetEtagHeader(this HttpResponseMessage response)
        {
            return EtagHeaderToEtag(response.Headers.ETag.Tag);
        }

        public static long? GetEtagHeader(this GetResponse response)
        {
            return EtagHeaderToEtag(response.Headers[Constants.MetadataEtagField]);
        }

        public static long? GetEtagHeader(this HttpJsonRequest request)
        {
            return EtagHeaderToEtag(request.ResponseHeaders[Constants.MetadataEtagField]);
        }

        internal static long EtagHeaderToEtag(string responseHeader)
        {
            if (string.IsNullOrEmpty(responseHeader))
                throw new InvalidOperationException("Response didn't had an ETag header");

            if (responseHeader[0] == '\"')
                return long.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

            return long.Parse(responseHeader);
        }
    }
}
