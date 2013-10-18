using System;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Client.Connection.Async
{

    internal static class HttpResponseMessageExtensions
    {
        internal static string GetSingleHeaderValue(this HttpResponseMessage httpResponseMessage, string header)
        {
            return httpResponseMessage.Headers.GetValues(header).Single();
        }

        internal static bool GetSingleHeaderValueAsBool(this HttpResponseMessage httpResponseMessage, string header)
        {
            return bool.Parse(httpResponseMessage.Headers.GetValues(header).Single());
        }

        internal static int GetSingleHeaderValueAsInt(this HttpResponseMessage httpResponseMessage, string header)
        {
            return int.Parse(httpResponseMessage.Headers.GetValues(header).Single());
        }

        internal static Etag GetSingleHeaderValueAsEtag(this HttpResponseMessage httpResponseMessage, string header)
        {
            return Etag.Parse(httpResponseMessage.Headers.GetValues(header).Single());
        }

        internal static DateTime GetSingleHeaderValueAsDateTime(this HttpResponseMessage httpResponseMessage, string header)
        {
            return DateTime.ParseExact(
                httpResponseMessage.Headers.GetValues(header).Single(),
                Default.DateTimeFormatsToRead,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None);
        }
    }
}