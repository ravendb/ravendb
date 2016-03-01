//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Net.Http;

namespace Raven.Abstractions.Extensions
{
    /// <summary>
    /// Extensions for handling metadata
    /// </summary>
    public static class MetadataExtensions
    {
        private static readonly HashSet<string> HeadersToIgnoreClient = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // Raven internal headers
            Constants.RavenServerBuild,
            "Raven-Client-Version",
            "Non-Authoritative-Information",
            "Raven-Timer-Request",
            "Raven-Authenticated-User",
            Constants.RavenLastModified,
            "Has-Api-Key",

            // COTS
            "Access-Control-Allow-Origin",
            "Access-Control-Max-Age",
            "Access-Control-Allow-Methods",
            "Access-Control-Request-Headers",
            "Access-Control-Allow-Headers",

            //proxy
            "Reverse-Via",
            "Persistent-Auth",
            "Allow",
            "Content-Disposition",
            "Content-Encoding",
            "Content-Language",
            "Content-Location",
            "Content-MD5",
            "Content-Range",
            "Content-Type",
            "Expires",
            // ignoring this header, we handle this internally
            Constants.LastModified,
            // Ignoring this header, since it may
            // very well change due to things like encoding,
            // adding metadata, etc
            "Content-Length",
            // Special things to ignore
            "Keep-Alive",
            "X-Powered-By",
            "X-AspNet-Version",
            "X-Requested-With",
            "X-SourceFiles",
            // Request headers
            "Accept-Charset",
            "Accept-Encoding",
            "Accept",
            "Accept-Language",
            "Authorization",
            "Cookie",
            "Expect",
            "From",
            "Host",
            "If-Match",
            "If-Modified-Since",
            "If-None-Match",
            "If-Range",
            "If-Unmodified-Since",
            "Max-Forwards",
            "Referer",
            "TE",
            "User-Agent",
            "DNT",
            //Response headers
            "Accept-Ranges",
            "Age",
            "Allow",
            Constants.MetadataEtagField,
            "Location",
            "Retry-After",
            "Server",
            "Set-Cookie2",
            "Set-Cookie",
            "Vary",
            "Www-Authenticate",
            // General
            "Cache-Control",
            "Connection",
            "Date",
            "Pragma",
            "Trailer",
            "Transfer-Encoding",
            "Upgrade",
            "Via",
            "Warning",

            // IIS Application Request Routing Module
            "X-ARR-LOG-ID",
            "X-ARR-SSL",
            "X-Forwarded-For",
            "X-Original-URL",

            // Azure specific
            "X-LiveUpgrade",
            "DISGUISED-HOST",
            "X-SITE-DEPLOYMENT-ID",
        };

        private static readonly HashSet<string> HeadersToIgnoreForClient = new HashSet<string>(HeadersToIgnoreClient.Except(new[]
        {
            Constants.LastModified,
            Constants.RavenLastModified
        }), StringComparer.OrdinalIgnoreCase);

        private static readonly HashSet<string> PrefixesInHeadersToIgnoreClient = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                                                               {
                                                                                   "Temp",
                                                                                   "X-NewRelic"
                                                                               };

        /// <summary>
        /// Filters the headers from unwanted headers
        /// </summary>
        /// <param name="self">HttpHeaders to filter</param>
        /// <param name="headersToIgnore">Headers to ignore</param>
        /// <param name="prefixesInHeadersToIgnore">Header prefixes to ignore</param>
        /// <returns></returns>
        public static RavenJObject FilterHeadersToObject(this RavenJObject self, HashSet<string> headersToIgnore, HashSet<string> prefixesInHeadersToIgnore)
        {
            if (self == null)
                return null;

            var metadata = new RavenJObject(self.Comparer);
            foreach (var header in self)
            {
                if (prefixesInHeadersToIgnore.Any(prefix => header.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                    continue;
                if (header.Key == Constants.DocumentIdFieldName)
                    continue;
                if (headersToIgnore.Contains(header.Key))
                    continue;
                var headerName = CaptureHeaderName(header.Key);
                metadata[headerName] = header.Value;
            }
            return metadata;
        }

        /// <summary>
        /// Filters the headers from unwanted headers
        /// </summary>
        /// <param name="self">The self.</param>
        /// <returns></returns>public static RavenJObject FilterHeadersToObject(this System.Collections.Specialized.NameValueCollection self, bool isServerDocument)
        public static RavenJObject FilterHeadersToObject(this RavenJObject self)
        {
            return FilterHeadersToObject(self, HeadersToIgnoreForClient, PrefixesInHeadersToIgnoreClient);
        }

        [Obsolete("Use RavenFS instead.")]
        public static RavenJObject FilterHeadersAttachment(this NameValueCollection self)
        {
            var filterHeaders = self.FilterHeadersToObject();
            if (self["Content-Type"] != null)
                filterHeaders["Content-Type"] = self["Content-Type"];
            return filterHeaders;
        }

        /// <summary>
        /// Filters the headers from unwanted headers
        /// </summary>
        /// <param name="self">The self.</param>
        /// <returns></returns>public static RavenJObject FilterHeadersToObject(this System.Collections.Specialized.NameValueCollection self, bool isServerDocument)
        public static RavenJObject FilterHeadersToObject(this NameValueCollection self)
        {
            var metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase);
            foreach (string header in self)
            {
                try
                {
                    if (PrefixesInHeadersToIgnoreClient.Any(prefix => header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                        continue;
                    if (HeadersToIgnoreForClient.Contains(header))
                        continue;
                    var valuesNonDistinct = self.GetValues(header);
                    if (valuesNonDistinct == null)
                        continue;
                    var values = new HashSet<string>(valuesNonDistinct);
                    var headerName = CaptureHeaderName(header);
                    if (values.Count == 1)
                        metadata[headerName] = GetValueWithDates(values.First());
                    else
                        metadata[headerName] = new RavenJArray(values.Select(GetValueWithDates));
                }
                catch (Exception exc)
                {
                    throw new JsonReaderException(string.Concat("Unable to Filter Header: ", header), exc);
                }
            }
            return metadata;
        }

        [Obsolete("Use RavenFS instead.")]
        public static RavenJObject FilterHeadersAttachment(this IEnumerable<KeyValuePair<string, IEnumerable<string>>> self)
        {
            var filterHeaders = self.FilterHeadersToObject();
            string contentType = null;
            foreach (var keyValue in self)
            {
                if (keyValue.Key.Equals("Content-Type"))
                {
                    contentType = keyValue.Value.FirstOrDefault();
                    break;
                }
            }
            if (contentType != null)
                filterHeaders["Content-Type"] = contentType;

            return filterHeaders;
        }

        /// <summary>
        /// Filters the headers from unwanted headers
        /// </summary>
        /// <param name="self">HttpHeaders to filter</param>
        /// <param name="headersToIgnore">Headers to ignore</param>
        /// <param name="prefixesInHeadersToIgnore">Header prefixes to ignore</param>
        /// <returns></returns>
        public static RavenJObject FilterHeadersToObject(this IEnumerable<KeyValuePair<string, IEnumerable<string>>> self, HashSet<string> headersToIgnore, HashSet<string> prefixesInHeadersToIgnore)
        {
            var metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase);
            foreach (var a in self)
            {
                var header = a.Key;
                try
                {
                    if (prefixesInHeadersToIgnore.Any(prefix => header.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                        continue;
                    if (headersToIgnore.Contains(header))
                        continue;
                    var valuesNonDistinct = a.Value;
                    if (valuesNonDistinct == null)
                        continue;
                    var values = new HashSet<string>(valuesNonDistinct);
                    var headerName = CaptureHeaderName(header);
                    if (values.Count == 1)
                        metadata[headerName] = GetValueWithDates(values.First());
                    else
                        metadata[headerName] = new RavenJArray(values.Select(GetValueWithDates));
                }
                catch (Exception exc)
                {
                    throw new JsonReaderException(string.Concat("Unable to Filter Header: ", header), exc);
                }
            }
            return metadata;
        }

        /// <summary>
        /// Filters the headers from unwanted headers
        /// </summary>
        /// <param name="self">The self.</param>
        /// <returns></returns>
        public static RavenJObject FilterHeadersToObject(this IEnumerable<KeyValuePair<string, IEnumerable<string>>> self)
        {
            return FilterHeadersToObject(self, HeadersToIgnoreClient, PrefixesInHeadersToIgnoreClient);
        }

        public static RavenJObject HeadersToObject(this HttpResponseMessage self)
        {
            var metadata = new RavenJObject(StringComparer.OrdinalIgnoreCase);

            var headers = self.Headers.Concat(self.Content.Headers);
            foreach (var a in headers)
            {
                var header = a.Key;

                try
                {
                    var headerName = CaptureHeaderName(header);

                    var valuesNonDistinct = a.Value;
                    if (valuesNonDistinct == null)
                        continue;

                    var values = new HashSet<string>(valuesNonDistinct);
                    if (values.Count == 1)
                    {
                        metadata[headerName] = GetValue(values.FirstOrDefault());
                    }
                    else
                    {
                        metadata[headerName] = new RavenJArray(values.Select(x => GetValue(x)));
                    }
                }
                catch (Exception exc)
                {
                    throw new JsonReaderException(string.Concat("Unable to build header: ", header), exc);
                }
            }

            return metadata;
        }

        private static string CaptureHeaderName(string header)
        {
            var lastWasDash = true;
            var sb = new StringBuilder(header.Length);

            foreach (var ch in header)
            {
                sb.Append(lastWasDash ? char.ToUpper(ch) : ch);

                lastWasDash = ch == '-';
            }

            return sb.ToString();
        }

        private static RavenJToken GetValue(string val)
        {
            try
            {
                if (val.StartsWith("{"))
                    return RavenJObject.Parse(val);
                if (val.StartsWith("["))
                    return RavenJArray.Parse(val);

                return new RavenJValue(Uri.UnescapeDataString(val));
            }
            catch (Exception exc)
            {
                throw new JsonReaderException(string.Concat("Unable to get value: ", val), exc);
            }
        }

        private static RavenJToken GetValueWithDates(string val)
        {
            try
            {
                if (val.StartsWith("{"))
                    return RavenJObject.Parse(val);
                if (val.StartsWith("["))
                    return RavenJArray.Parse(val);

                DateTime dateTime;
                if (DateTime.TryParseExact(val, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTime))
                {
                    if (val.EndsWith("Z"))
                        return DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                    return new RavenJValue(dateTime);
                }

                DateTimeOffset dateTimeOffset;
                if (DateTimeOffset.TryParseExact(val, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dateTimeOffset))
                    return new RavenJValue(dateTimeOffset);

                return new RavenJValue(Uri.UnescapeDataString(val));
            }
            catch (Exception exc)
            {
                throw new JsonReaderException(string.Concat("Unable to get value: ", val), exc);
            }
        }
    }
}
