//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Database.Server.RavenFS.Extensions
{

	/// <summary>
	/// Extensions for handling metadata
	/// </summary>
	public static class MetadataExtensions
	{
		public static void AddHeaders(HttpResponseMessage context, FileAndPages fileAndPages)
		{
            foreach (var item in fileAndPages.Metadata)
            {
                if (item.Key == "ETag")
                {
                    var etag = item.Value.Value<Guid>();
                    if (etag == null)
                        continue;

                    context.Headers.ETag = new EntityTagHeaderValue(@"""" + etag + @"""");
                }
                else
                {                                        
                    if (item.Key == "Last-Modified")
                    {
                        string value = item.Value.Value<string>();
                        context.Content.Headers.Add(item.Key, new Regex("\\.\\d{5}").Replace(value, string.Empty)); // HTTP does not provide milliseconds, so remove it
                    }
                    else
                    {
                        string value;
                        switch( item.Value.Type )
                        {
                            case JTokenType.Object:
                            case JTokenType.Array: 
                                value = item.Value.ToString(Imports.Newtonsoft.Json.Formatting.None);
                                break;
                            default:
                                value = item.Value.Value<string>();
                                break;
                        }                      
                        context.Content.Headers.Add(item.Key, value);
                    }
                }
            }
		}

        public static RavenJObject WithETag(this RavenJObject metadata, Guid etag)
        {
            metadata["ETag"] = new RavenJValue(etag);
            return metadata;
        }

        public static RavenJObject DropRenameMarkers(this RavenJObject metadata)
		{
			metadata.Remove(SynchronizationConstants.RavenDeleteMarker);
			metadata.Remove(SynchronizationConstants.RavenRenameFile);

			return metadata;
		}

        public static RavenJObject WithRenameMarkers(this RavenJObject metadata, string rename)
		{
			metadata[SynchronizationConstants.RavenDeleteMarker] = "true";
			metadata[SynchronizationConstants.RavenRenameFile] = rename;

			return metadata;
		}

        public static RavenJObject WithDeleteMarker(this RavenJObject metadata)
		{
			metadata[SynchronizationConstants.RavenDeleteMarker] = "true";

			return metadata;
		}

		public static T Value<T>(this HttpHeaders self, string name)
		{
			var value = self.GetValues(name).First();
			return new JsonSerializer().Deserialize<T>(new JsonTextReader(new StringReader(value)));
		}

        public static void AddHeaders(this HttpWebRequest request, RavenJObject metadata)
        {
            foreach (var item in metadata)
            {
                request.Headers[item.Key] = item.Value.ToString();
            }
        }

        public static void AddHeaders(this HttpWebRequest request, NameValueCollection metadata)
        {
            foreach (var key in metadata.AllKeys)
            {
                var values = metadata.GetValues(key);
                if (values == null)
                    continue;
                foreach (var value in values)
                {
                    request.Headers[key] = value;
                }
            }
        }

		private static readonly HashSet<string> HeadersToIgnoreClient = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
		{
			// Raven internal headers
			"Raven-Server-Build",
			"Non-Authoritive-Information",
			"Raven-Timer-Request",

            //proxy
            "Reverse-Via",

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
			"Last-Modified",
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
			//Response headers
			"Accept-Ranges",
			"Age",
			"Allow",
			"ETag",
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
		};

		public static readonly IList<string> ReadOnlyHeaders = new List<string> { "Last-Modified", "ETag" }.AsReadOnly();

		public static NameValueCollection FilterHeadersForViewing(this NameValueCollection metadata)
		{
			var filteredHeaders = metadata.FilterHeaders();

			foreach (var header in ReadOnlyHeaders)
			{
				var value = metadata[header];
				if (value != null)
				{
					filteredHeaders.Add(header, value);
				}
			}

			return filteredHeaders;
		}

		/// <summary>
		/// Filters the headers from unwanted headers
		/// </summary>
		/// <param name="self">The self.</param>
		public static NameValueCollection FilterHeaders(this NameValueCollection self)
		{
			var metadata = new NameValueCollection();
			foreach (string header in self)
			{
				if (header.StartsWith("Temp"))
					continue;
				if (HeadersToIgnoreClient.Contains(header))
					continue;
				var values = self.GetValues(header);
				var headerName = CaptureHeaderName(header);

				if (values == null)
					continue;

				foreach (var value in values)
				{
					metadata.Add(headerName, value);
				}
			}
			return metadata;
		}

		public static T Value<T>(this NameValueCollection self, string key)
		{
			return new JsonSerializer().Deserialize<T>(new JsonTextReader(new StringReader(self[key])));
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
	}
}