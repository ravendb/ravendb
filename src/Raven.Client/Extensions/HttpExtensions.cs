// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;

namespace Raven.Client.Extensions
{
    internal static class HttpExtensions
    {
        public static string GetRequiredEtagHeader(this HttpResponseMessage response)
        {
            if (response.Headers.TryGetValues(Constants.Headers.Etag, out IEnumerable<string> values) == false ||
                values == null)
                throw new InvalidOperationException("Response didn't had an ETag header");

            var value = values.FirstOrDefault();
            if (value == null)
                throw new InvalidOperationException("Response didn't had an ETag header");

            return EtagHeaderToChangeVector(value);
        }

        public static string GetEtagHeader(this HttpResponseMessage response)
        {
            if (response.Headers.TryGetValues(Constants.Headers.Etag, out IEnumerable<string> values) == false ||
                values == null)
                return null;

            var value = values.FirstOrDefault();
            if (value == null)
                return null;

            return EtagHeaderToChangeVector(value);
        }

        public static string GetEtagHeader(this Dictionary<string, string> headers)
        {
            string value;
            if (headers.TryGetValue(Constants.Headers.Etag, out value) == false || value == null)
                return null;

            return EtagHeaderToChangeVector(value);
        }

        public static bool? GetBoolHeader(this HttpResponseMessage response, string header)
        {
            if (response.Headers.TryGetValues(header, out IEnumerable<string> values) == false || values == null)
                return null;

            var value = values.FirstOrDefault();
            if (value == null)
                return null;

            return bool.Parse(value);
        }

        private static string EtagHeaderToChangeVector(string responseHeader)
        {
            if (string.IsNullOrEmpty(responseHeader))
                throw new InvalidOperationException("Response didn't had an ETag header");

            if (responseHeader[0] == '\"')
                return responseHeader.Substring(1, responseHeader.Length - 2);

            return responseHeader;
        }
        
        public static bool IsSuccessStatusCode(this HttpStatusCode statusCode)
        {
            return (int)statusCode >= 200 && (int)statusCode <= 299;
        }
    }
}
