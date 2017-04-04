// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Net.Http;
using Raven.Abstractions.Data;

namespace Raven.Client.Connection
{
    public static class HttpExtensions
    {
        public static Etag GetEtagHeader(this NameValueCollection headers)
        {
            var values = headers.GetValues(Constants.MetadataEtagField);

            var value = values?.FirstOrDefault();
            return value == null ? null : EtagHeaderToEtag(value);
        }

#if !DNXCORE50
        public static Etag GetEtagHeader(this HttpWebResponse response)
        {
            return EtagHeaderToEtag(response.GetResponseHeader(Constants.MetadataEtagField));
        }
#endif

        public static Etag GetEtagHeader(this HttpResponseMessage response)
        {
            return EtagHeaderToEtag(response.Headers.ETag.Tag);
        }

        public static Etag GetEtagHeader(this GetResponse response)
        {
            string etag;
            response.Headers.TryGetValue(Constants.MetadataEtagField, out etag);
            return EtagHeaderToEtag(etag);
        }

        internal static Etag EtagHeaderToEtag(string responseHeader)
        {
            if (string.IsNullOrEmpty(responseHeader))
                throw new InvalidOperationException("Response didn't have an ETag header");

            if (responseHeader[0] == '\"')
                return Etag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

            return Etag.Parse(responseHeader);
        }
    }
}
