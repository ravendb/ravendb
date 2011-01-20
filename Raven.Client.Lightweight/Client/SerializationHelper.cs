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

namespace Raven.Client.Client
{
	internal class SerializationHelper
	{
		public static IEnumerable<JsonDocument> JObjectsToJsonDocuments(IEnumerable<JObject> responses)
		{
			return (from doc in responses
			        let metadata = (JObject)doc["@metadata"]
			        let _ = doc.Remove("@metadata")
			        select new JsonDocument
			        {
			        	Key = metadata["@id"].Value<string>(),
			        	LastModified = DateTime.ParseExact(metadata["Last-Modified"].Value<string>(), "r", CultureInfo.InvariantCulture),
			        	Etag = new Guid(metadata["@etag"].Value<string>()),
			        	NonAuthoritiveInformation = metadata.Value<bool>("Non-Authoritive-Information"),
			        	Metadata = metadata,
			        	DataAsJson = doc,
			        });
		}

	}
}
