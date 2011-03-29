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
	internal static class SerializationHelper
	{
		public static IEnumerable<JsonDocument> RavenJObjectsToJsonDocuments(IEnumerable<RavenJObject> responses)
		{
			return (from doc in responses
			        let metadata = (RavenJObject)doc.Properties["@metadata"]
			        let _ = doc.Properties.Remove("@metadata")
			        select new JsonDocument
			        {
			        	Key = metadata["@id"].Value<string>(),
			        	LastModified = DateTime.ParseExact(metadata["Last-Modified"].Value<string>(), "r", CultureInfo.InvariantCulture),
			        	Etag = new Guid(metadata["@etag"].Value<string>()),
			        	NonAuthoritiveInformation = metadata.Value<bool>("Non-Authoritive-Information"),
			        	Metadata = metadata.FilterHeaders(isServerDocument: false),
			        	DataAsJson = doc,
			        });
		}

		public static IEnumerable<JsonDocument> ToJsonDocuments(this IEnumerable<RavenJObject> responses)
		{
			return RavenJObjectsToJsonDocuments(responses);
		}

		public static JsonDocument ToJsonDocument(this RavenJObject response)
		{
			return RavenJObjectsToJsonDocuments(new[] { response }).First();
		}
	}
}
