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
using Raven.Json.Linq;

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
		public static IEnumerable<JsonDocument> RavenJObjectsToJsonDocuments(IEnumerable<RavenJObject> responses)
		{
			return (from doc in responses
					let metadata = doc["@metadata"] as RavenJObject
					let _ = doc.Remove("@metadata")
					let key = (metadata != null) ? metadata["@id"].Value<string>() : ""
					let lastModified = (metadata != null) ? DateTime.ParseExact(metadata["Last-Modified"].Value<string>(), "r", CultureInfo.InvariantCulture).ToLocalTime() : DateTime.Now
					let etag = (metadata != null) ? new Guid(metadata["@etag"].Value<string>()) : Guid.Empty
					let nai = (metadata != null) ? metadata.Value<bool>("Non-Authoritive-Information") : false
					select new JsonDocument
					{
						Key = key,
						LastModified = lastModified,
						Etag = etag,
						NonAuthoritiveInformation = nai,
						Metadata = metadata.FilterHeaders(isServerDocument: false),
						DataAsJson = doc,
					});
		}

		///<summary>
		/// Translate a collection of JObject to JsonDocuments
		///</summary>
		public static IEnumerable<JsonDocument> ToJsonDocuments(this IEnumerable<RavenJObject> responses)
		{
			return RavenJObjectsToJsonDocuments(responses);
		}

		///<summary>
		/// Translate a collection of JObject to JsonDocuments
		///</summary>
		public static JsonDocument ToJsonDocument(this RavenJObject response)
		{
			return RavenJObjectsToJsonDocuments(new[] { response }).First();
		}
	}
}
