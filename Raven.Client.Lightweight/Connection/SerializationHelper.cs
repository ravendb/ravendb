//-----------------------------------------------------------------------
// <copyright file="SerializationHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Net;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	///<summary>
	/// Helper method to do serialization from RavenJObject to JsonDocument
	///</summary>
	public static class SerializationHelper
	{
		///<summary>
		/// Translate a collection of RavenJObject to JsonDocuments
		///</summary>
		public static IEnumerable<JsonDocument> RavenJObjectsToJsonDocuments(IEnumerable<RavenJObject> responses)
		{
			var list = new List<JsonDocument>();
			foreach (var doc in responses)
			{
				if(doc == null)
				{
					list.Add(null);
					continue;
				}
				var metadata = (RavenJObject)doc["@metadata"];
				doc.Remove("@metadata");
				var key = Extract(metadata, "@id", string.Empty);
				var lastModified = Extract(metadata, Constants.LastModified, SystemTime.Now, (string d) => ConvertToUtcDate(d));
				var etag = Extract(metadata, "@etag", Guid.Empty, (string g) => HttpExtensions.EtagHeaderToGuid(g));
				var nai = Extract(metadata, "Non-Authoritative-Information", false, (string b) => Convert.ToBoolean(b));
				list.Add(new JsonDocument
					{
						Key = key,
						LastModified = lastModified,
						Etag = etag,
						NonAuthoritativeInformation = nai,
						Metadata = metadata.FilterHeaders(),
						DataAsJson = doc,
					});
			}
			return list;
		}

		///<summary>
		/// Translate a collection of RavenJObject to JsonDocuments
		///</summary>
		public static IEnumerable<JsonDocument> ToJsonDocuments(this IEnumerable<RavenJObject> responses)
		{
			return RavenJObjectsToJsonDocuments(responses);
		}

		///<summary>
		/// Translate a collection of RavenJObject to JsonDocuments
		///</summary>
		public static JsonDocument ToJsonDocument(this RavenJObject response)
		{
			return RavenJObjectsToJsonDocuments(new[] { response }).First();
		}

		static DateTime ConvertToUtcDate(string date)
		{
			return DateTime.SpecifyKind( DateTime.ParseExact(date, new[]{"r","o"}, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind), DateTimeKind.Utc);
		}

		static T Extract<T>(RavenJObject metadata, string key, T defaultValue = default(T))
		{
			return Extract<T, T>(metadata, key, defaultValue, t => t);
		}

		static TResult Extract<T, TResult>(RavenJObject metadata, string key, TResult defaultValue, Func<T, TResult> convert)
		{
			if (metadata == null) return defaultValue;
			if (!metadata.ContainsKey(key)) return defaultValue;

			var value = metadata[key].Value<object>();

			if(value is TResult)
				return (TResult) value;

			return convert(metadata[key].Value<T>());
		}

		/// <summary>
		/// Translate a result for a query
		/// </summary>
		public static QueryResult ToQueryResult(RavenJObject json, Guid etag)
		{
			var result = new QueryResult
			             	{
			             		IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
			             		IndexTimestamp = json.Value<DateTime>("IndexTimestamp"),
			             		IndexEtag = etag,
			             		Results = ((RavenJArray) json["Results"]).Cast<RavenJObject>().ToList(),
			             		Includes = ((RavenJArray) json["Includes"]).Cast<RavenJObject>().ToList(),
			             		TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
			             		IndexName = json.Value<string>("IndexName"),
			             		SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString()),
			             	};


			if (json.ContainsKey("NonAuthoritativeInformation"))
				result.NonAuthoritativeInformation = Convert.ToBoolean(json["NonAuthoritativeInformation"].ToString());

			return result;
		}

		/// <summary>
		/// Deserialize a request to a JsonDocument
		/// </summary>
		public static JsonDocument DeserializeJsonDocument(string key, RavenJToken requestJson,
#if !SILVERLIGHT
			NameValueCollection headers, 
#else 
			IDictionary<string, IList<string>> headers,
#endif
			HttpStatusCode statusCode)
		{
			var jsonData = (RavenJObject)requestJson;
			var meta = headers.FilterHeaders();
			
#if !SILVERLIGHT
			var etag = headers["ETag"];
			var lastModified = headers[Constants.LastModified];
#else
			var etag = headers["ETag"].First();
			var lastModified = headers[Constants.LastModified].First();
#endif
			return new JsonDocument
			{
				DataAsJson = jsonData,
				NonAuthoritativeInformation = statusCode == HttpStatusCode.NonAuthoritativeInformation,
				Key = key,
				Etag = HttpExtensions.EtagHeaderToGuid(etag),
				LastModified = DateTime.ParseExact(lastModified, "r", CultureInfo.InvariantCulture).ToLocalTime(),
				Metadata = meta
			};
		}

		/// <summary>
		/// Deserialize a request to a JsonDocument
		/// </summary>
		public static JsonDocumentMetadata DeserializeJsonDocumentMetadata(string key,
#if !SILVERLIGHT
			NameValueCollection headers,
#else 
			IDictionary<string, IList<string>> headers,
#endif
			HttpStatusCode statusCode)
		{
			RavenJObject meta = null;
			try
			{
				meta = headers.FilterHeaders();
			}
			catch (JsonReaderException jre)
			{
				throw new JsonReaderException("Invalid Json Response", jre);
			}
#if !SILVERLIGHT
			var etag = headers["ETag"];
			var lastModified = headers[Constants.LastModified];
#else
			var etag = headers["ETag"].First();
			var lastModified = headers[Constants.LastModified].First();
#endif
			return new JsonDocumentMetadata
			{
				NonAuthoritativeInformation = statusCode == HttpStatusCode.NonAuthoritativeInformation,
				Key = key,
				Etag = HttpExtensions.EtagHeaderToGuid(etag),
				LastModified = DateTime.ParseExact(lastModified, "r", CultureInfo.InvariantCulture).ToLocalTime(),
				Metadata = meta
			};
		}
	}
}
