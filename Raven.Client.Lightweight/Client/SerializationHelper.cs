//-----------------------------------------------------------------------
// <copyright file="SerializationHelper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Client
{
	///<summary>
	/// Helper method to do serialization from JObject to JsonDocument
	///</summary>
	public static class SerializationHelper
	{
		///<summary>
		/// Translate a collection of JObject to JsonDocuments
		///</summary>
		public static IEnumerable<JsonDocument> JObjectsToJsonDocuments(IEnumerable<JObject> responses)
		{
			return (from doc in responses
					let metadata = doc["@metadata"] as JObject
					let _ = doc.Remove("@metadata")
					let key = Extract(metadata, "@id", string.Empty)
					let lastModified = Extract(metadata, "Last-Modified", DateTime.Now, (string d) => ConvertToUtcDate(d))
					let etag = Extract(metadata, "@etag", Guid.Empty, (string g) => Guid.Parse(g))
					let nai = Extract(metadata, "Non-Authoritive-Information", false, (string b) => Convert.ToBoolean(b))
					select new JsonDocument
					{
						Key = key,
						LastModified = lastModified,
						Etag = etag,
						NonAuthoritiveInformation = nai,
						Metadata = metadata.FilterHeaders(isServerDocument: false),
						DataAsJson = doc,
					}).ToList();
		}

		///<summary>
		/// Translate a collection of JObject to JsonDocuments
		///</summary>
		public static IEnumerable<JsonDocument> ToJsonDocuments(this IEnumerable<JObject> responses)
		{
			return JObjectsToJsonDocuments(responses);
		}

		///<summary>
		/// Translate a collection of JObject to JsonDocuments
		///</summary>
		public static JsonDocument ToJsonDocument(this JObject response)
		{
			return JObjectsToJsonDocuments(new[] { response }).First();
		}

		static DateTime ConvertToUtcDate(string date)
		{
			return DateTime.SpecifyKind( DateTime.ParseExact(date, "r", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind), DateTimeKind.Utc);
		}

		static T Extract<T>(IDictionary<string, JToken> metadata, string key, T defaultValue = default(T))
		{
			return Extract<T, T>(metadata, key, defaultValue, t => t);
		}

		static TResult Extract<T, TResult>(IDictionary<string, JToken> metadata, string key, TResult defaultValue, Func<T, TResult> convert)
		{
			if (metadata == null) return defaultValue;
			if (!metadata.ContainsKey(key)) return defaultValue;

			var value = metadata[key].Value<T>();

			return convert(value);
		}
	}
}
