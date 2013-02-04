//-----------------------------------------------------------------------
// <copyright file="SerializationHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
#if !SILVERLIGHT
using System.Collections.Specialized;
#else
using Raven.Client.Silverlight.MissingFromSilverlight;
#endif
using System.Globalization;
using System.Linq;
using System.Net;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json.Linq;
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
				if (doc == null)
				{
					list.Add(null);
					continue;
				}
				var metadata = (RavenJObject)doc["@metadata"];
				doc.Remove("@metadata");
				var key = Extract(metadata, "@id", string.Empty);

				var lastModified = GetLastModified(metadata);

				var etag = Extract(metadata, "@etag", Guid.Empty, (string g) => HttpExtensions.EtagHeaderToGuid(g));
				var nai = Extract(metadata, "Non-Authoritative-Information", false, (string b) => Convert.ToBoolean(b));
				list.Add(new JsonDocument
				{
					Key = key,
					LastModified = lastModified,
					Etag = etag,
					TempIndexScore = metadata == null ? null : metadata.Value<float?>(Constants.TemporaryScoreValue),
					NonAuthoritativeInformation = nai,
					Metadata = metadata.FilterHeaders(),
					DataAsJson = doc,
				});
			}
			return list;
		}

		private static DateTime GetLastModified(RavenJObject metadata)
		{
			if (metadata == null)
				return SystemTime.UtcNow;
			return metadata.ContainsKey(Constants.RavenLastModified) ?
					   Extract(metadata, Constants.RavenLastModified, SystemTime.UtcNow, (string d) => ConvertToUtcDate(d)) :
					   Extract(metadata, Constants.LastModified, SystemTime.UtcNow, (string d) => ConvertToUtcDate(d));
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

		private static DateTime ConvertToUtcDate(string date)
		{
			return DateTime.SpecifyKind(DateTime.ParseExact(date, new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind), DateTimeKind.Utc);
		}

		private static T Extract<T>(RavenJObject metadata, string key, T defaultValue = default(T))
		{
			return Extract<T, T>(metadata, key, defaultValue, t => t);
		}

		private static TResult Extract<T, TResult>(RavenJObject metadata, string key, TResult defaultValue, Func<T, TResult> convert)
		{
			if (metadata == null) return defaultValue;
			if (!metadata.ContainsKey(key)) return defaultValue;
			if (metadata[key].Type == JTokenType.Array) return defaultValue;

			var value = metadata[key].Value<object>();

			if (value is TResult)
				return (TResult)value;

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
				Results = ((RavenJArray)json["Results"]).Cast<RavenJObject>().ToList(),
				Includes = ((RavenJArray)json["Includes"]).Cast<RavenJObject>().ToList(),
				TotalResults = Convert.ToInt32(json["TotalResults"].ToString()),
				IndexName = json.Value<string>("IndexName"),
				SkippedResults = Convert.ToInt32(json["SkippedResults"].ToString()),
				Highlightings = (json.Value<RavenJObject>("Highlightings") ?? new RavenJObject())
					.JsonDeserialization<Dictionary<string, Dictionary<string, string[]>>>()
			};

			if (json.ContainsKey("NonAuthoritativeInformation"))
				result.NonAuthoritativeInformation = Convert.ToBoolean(json["NonAuthoritativeInformation"].ToString());

			return result;
		}

		/// <summary>
		/// Deserialize a request to a JsonDocument
		/// </summary>
		public static JsonDocument DeserializeJsonDocument(string key, RavenJToken requestJson,
														   NameValueCollection headers,
														   HttpStatusCode statusCode)
		{
			var jsonData = (RavenJObject)requestJson;
			var meta = headers.FilterHeaders();

			var etag = headers["ETag"];

			return new JsonDocument
			{
				DataAsJson = jsonData,
				NonAuthoritativeInformation = statusCode == HttpStatusCode.NonAuthoritativeInformation,
				Key = key,
				Etag = HttpExtensions.EtagHeaderToGuid(etag),
				LastModified = GetLastModifiedDate(headers),
				Metadata = meta
			};
		}

		private static DateTime? GetLastModifiedDate(NameValueCollection headers)
		{
			var lastModified = headers.GetValues(Constants.RavenLastModified);
			if (lastModified == null || lastModified.Length != 1)
			{
				var dt = DateTime.ParseExact(headers[Constants.LastModified], new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
				return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
			}
			return DateTime.ParseExact(lastModified[0], new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
		}

		/// <summary>
		/// Deserialize a request to a JsonDocument
		/// </summary>
		public static JsonDocumentMetadata DeserializeJsonDocumentMetadata(string key,
																		   NameValueCollection headers,
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
			var etag = headers["ETag"];
			string lastModified = headers[Constants.RavenLastModified] ?? headers[Constants.LastModified];
			var dateTime = DateTime.ParseExact(lastModified, new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
			var lastModifiedDate = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);

			return new JsonDocumentMetadata
			{
				NonAuthoritativeInformation = statusCode == HttpStatusCode.NonAuthoritativeInformation,
				Key = key,
				Etag = HttpExtensions.EtagHeaderToGuid(etag),
				LastModified = lastModifiedDate,
				Metadata = meta
			};
		}
	}
}