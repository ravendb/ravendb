// -----------------------------------------------------------------------
//  <copyright file="HttpExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Client.Connection
{
    public static class HttpExtensions
    {
        public static long? GetEtagHeader(this HttpResponseMessage response)
        {
            IEnumerable<string> values;
            if (response.Headers.TryGetValues(Constants.MetadataEtagField, out values) == false || values == null)
                return null;

            var value = values.FirstOrDefault();
            if (value == null)
                return null;

            return EtagHeaderToEtag(value);
        }

        public static long? GetEtagHeader(this BlittableJsonReaderObject headers)
        {
            string value;
            if (headers.TryGet(Constants.MetadataEtagField, out value) == false || value == null)
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
