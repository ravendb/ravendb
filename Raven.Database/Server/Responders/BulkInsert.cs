using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using NetTopologySuite.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class BulkInsert : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/bulkInsert$"; }
		}
		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}
		public override void Respond(IHttpContext context)
		{
			var documents = 0;
			while (true)
			{
				var binaryReader = new BinaryReader(context.Request.InputStream);
				int count;
				try
				{
					count = binaryReader.ReadInt32();
				}
				catch (EndOfStreamException)
				{
					break;
				}
				documents += Database.BulkInsert(YieldDocumentsInBatch(binaryReader, count));
			}

			context.Log(log => log.Debug("\tBulk inserted {0:#,#;;0} documents", documents));

			context.WriteJson(new
			{
				Documents = documents
			});
		}

		private static IEnumerable<JsonDocument> YieldDocumentsInBatch(BinaryReader reader, int count)
		{
			for (int i = 0; i < count; i++)
			{
				var doc = (RavenJObject)RavenJToken.ReadFrom(new BsonReader(reader));

				var metadata = doc.Value<RavenJObject>("@metadata");

				if (metadata == null)
					throw new InvalidOperationException("Could not find metadata for document");

				var id = metadata.Value<string>("@id");
				if (string.IsNullOrEmpty(id))
					throw new InvalidOperationException("Could not get id from metadata");

				doc.Remove("@metadata");

				yield return new JsonDocument
				{
					Key = id,
					DataAsJson = doc,
					Metadata = metadata
				};
			}
		}
	}
}