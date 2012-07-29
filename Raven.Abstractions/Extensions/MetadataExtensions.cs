//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

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
			"Raven-Server-Build",
			"Raven-Client-Version",
			"Non-Authoritative-Information",
			"Raven-Timer-Request",
			"Raven-Authenticated-User",

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

		/// <summary>
		/// Filters the headers from unwanted headers
		/// </summary>
		/// <param name="self">The self.</param>
		/// <param name="isServerDocument">if set to <c>true</c> [is server document].</param>
		/// <returns></returns>public static RavenJObject FilterHeaders(this System.Collections.Specialized.NameValueCollection self, bool isServerDocument)
		public static RavenJObject FilterHeaders(this RavenJObject self)
		{
			if (self == null)
				return self;

			var metadata = new RavenJObject();
			foreach (var header in self)
			{
				if(header.Key.StartsWith("Temp"))
					continue;
				if(header.Key == Constants.DocumentIdFieldName)
					continue;
				if (HeadersToIgnoreClient.Contains(header.Key))
					continue;
				var headerName = CaptureHeaderName(header.Key);
				metadata[headerName] = header.Value;
			}
			return metadata;
		}

#if SILVERLIGHT
		public static RavenJObject FilterHeadersAttachment(this IDictionary<string, IList<string>> self)
		{
			var filterHeaders = self.FilterHeaders();
			if (self.ContainsKey("Content-Type"))
				filterHeaders["Content-Type"] = self["Content-Type"].FirstOrDefault();
			return filterHeaders;
		}

		/// <summary>
		/// Filters the headers from unwanted headers
		/// </summary>
		/// <param name="self">The self.</param>
		/// <param name="isServerDocument">if set to <c>true</c> [is server document].</param>
		/// <returns></returns>public static RavenJObject FilterHeaders(this System.Collections.Specialized.NameValueCollection self, bool isServerDocument)
		public static RavenJObject FilterHeaders(this IDictionary<string, IList<string>> self)
		  {
			  var metadata = new RavenJObject();
			foreach (var header in self)
			{
				if (header.Key.StartsWith("Temp"))
					continue;
				if (HeadersToIgnoreClient.Contains(header.Key))
					continue;
				var values = header.Value;
				var headerName = CaptureHeaderName(header.Key);
				if (values.Count == 1)
					metadata.Add(headerName, GetValue(values[0]));
				else
					metadata.Add(headerName, new RavenJArray(values.Select(GetValue)));
			}
			return metadata;
		}
#else
		public static RavenJObject FilterHeadersAttachment(this NameValueCollection self)
		{
			var filterHeaders = self.FilterHeaders();
			if (self["Content-Type"] != null)
				filterHeaders["Content-Type"] = self["Content-Type"];
			return filterHeaders;
		}

		/// <summary>
		/// Filters the headers from unwanted headers
		/// </summary>
		/// <param name="self">The self.</param>
		/// <returns></returns>public static RavenJObject FilterHeaders(this System.Collections.Specialized.NameValueCollection self, bool isServerDocument)
		public static RavenJObject FilterHeaders(this NameValueCollection self)
		{
			var metadata = new RavenJObject(StringComparer.InvariantCultureIgnoreCase);
			foreach (string header in self)
			{
				try
				{
					if(header.StartsWith("Temp"))
						continue;
					if (HeadersToIgnoreClient.Contains(header))
						continue;
					var valuesNonDistinct = self.GetValues(header);
					if(valuesNonDistinct == null)
						continue;
					var values = new HashSet<string>(valuesNonDistinct);
					var headerName = CaptureHeaderName(header);
					if (values.Count == 1)
						metadata[headerName] = GetValue(values.First());
					else
						metadata[headerName] = new RavenJArray(values.Select(GetValue).Take(15));
				}
				catch (Exception exc)
				{
					throw new JsonReaderException(string.Concat("Unable to Filter Header: ", header), exc);
				}
			}
			return metadata;
		}
#endif

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
				DateTime result;
				if (DateTime.TryParseExact(val, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
					return new RavenJValue(result);
				return new RavenJValue(val);

			}
			catch (Exception exc)
			{
				throw new JsonReaderException(string.Concat("Unable to get value: ", val), exc);
			}

		}
	}
}
