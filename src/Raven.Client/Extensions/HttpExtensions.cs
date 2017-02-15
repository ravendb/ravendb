// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;

namespace Raven.Client.Extensions
{
    internal static class HttpExtensions
    {
        public static long? GetEtagHeader(this HttpResponseMessage response)
        {
            IEnumerable<string> values;
            if (response.Headers.TryGetValues(Constants.Headers.Etag, out values) == false || values == null)
                return null;

            var value = values.FirstOrDefault();
            if (value == null)
                return null;

            return EtagHeaderToEtag(value);
        }

        public static long? GetEtagHeader(this Dictionary<string, string> headers)
        {
            string value;
            if (headers.TryGetValue(Constants.Headers.Etag, out value) == false || value == null)
                return null;

            return EtagHeaderToEtag(value);
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
