using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using NetTopologySuite.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Abstractions;
using Raven.Database.Util.Streams;
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
			var options = new BulkInsertOptions
			{
				CheckForUpdates = context.GetCheckForUpdates(),
				CheckReferencesInIndexes = context.GetCheckReferencesInIndexes()
			};

			var sp = Stopwatch.StartNew();

			var documents = Database.BulkInsert(options, YieldBatches(context));

			context.Log(log => log.Debug("\tBulk inserted {0:#,#;;0} documents in {1}", documents, sp.Elapsed));

			context.WriteJson(new
			{
				Documents = documents
			});
		}

		private static IEnumerable<IEnumerable<JsonDocument>> YieldBatches(IHttpContext context)
		{
			using (var inputStream = context.Request.GetBufferLessInputStream())
			{
				var binaryReader = new BinaryReader(inputStream);
				while (true)
				{
					int size;
					try
					{
						size = binaryReader.ReadInt32();
					}
					catch (EndOfStreamException)
					{
						break;
					}
					using (var stream = new PartialStream(inputStream, size))
					{
						yield return YieldDocumentsInBatch(stream);
					}
				}
			}
		}

		private static IEnumerable<JsonDocument> YieldDocumentsInBatch(Stream partialStream)
		{
			using(var stream = new GZipStream(partialStream, CompressionMode.Decompress, leaveOpen:true))
			{
				var reader = new BinaryReader(stream);
				var count = reader.ReadInt32();
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
}