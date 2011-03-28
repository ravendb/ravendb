using Raven.Database.Data;

namespace Raven.Studio.Framework
{
	using System;
	using System.Collections.Generic;
	using System.Globalization;
	using System.Linq;
	using Database;
	using Newtonsoft.Json.Linq;

	public static class SerializationHelper
	{
		public static IEnumerable<JsonDocument> JObjectsToJsonDocuments(IEnumerable<JObject> responses)
		{
			return (from doc in responses
					let metadata = doc["@metadata"] as JObject
					let _ = doc.Remove("@metadata")
					let key = (metadata != null) ? metadata["@id"].Value<string>() : ""
					let lastModified = (metadata != null) ? DateTime.ParseExact(metadata["Last-Modified"].Value<string>(), "r", CultureInfo.InvariantCulture) : DateTime.Now
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

		public static IEnumerable<JsonDocument> ToJsonDocuments(this IEnumerable<JObject> responses)
		{
			return JObjectsToJsonDocuments(responses);
		}

		public static JsonDocument ToJsonDocument(this JObject response)
		{
			return JObjectsToJsonDocuments(new[] { response }).First();
		}
	}
}