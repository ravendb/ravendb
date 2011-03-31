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
			        let metadata = (JObject)doc["@metadata"]
			        let _ = doc.Remove("@metadata")
			        select new JsonDocument
			        {
			        	Key = metadata["@id"].Value<string>(),
						LastModified = DateTime.ParseExact(metadata["Last-Modified"].Value<string>(), "r", CultureInfo.InvariantCulture).ToLocalTime(),
			        	Etag = new Guid(metadata["@etag"].Value<string>()),
			        	NonAuthoritiveInformation = metadata.Value<bool>("Non-Authoritive-Information"),
			        	Metadata = metadata.FilterHeaders(isServerDocument: false),
			        	DataAsJson = doc,
			        });
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
	}
}
