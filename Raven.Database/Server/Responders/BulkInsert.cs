using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Server.Abstractions;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;

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
			if (string.IsNullOrEmpty(context.Request.QueryString["no-op"]) == false)
			{
				// this is a no-op request which is there just to force the client HTTP layer to handle the authentication
				// only used for legacy clients
				return; 
			}

			if (HttpContext.Current != null)
			{
				HttpContext.Current.Server.ScriptTimeout = 60*60*6; // six hours should do it, I think.
			}
			var options = new BulkInsertOptions
			{
				CheckForUpdates = context.GetCheckForUpdates(),
				CheckReferencesInIndexes = context.GetCheckReferencesInIndexes()
			};

			var operationId = ExtractOperationId(context);
			var sp = Stopwatch.StartNew();

			var status = new BulkInsertStatus();

			int documents = 0;
			var mre = new ManualResetEventSlim(false);

			var currentDatbase = Database;
			var task = Task.Factory.StartNew(() =>
			{
				currentDatbase.BulkInsert(options, YieldBatches(context, mre, batchSize => documents += batchSize), operationId);
			    status.Documents = documents;
			    status.Completed = true;
			});

			long id;
			Database.AddTask(task, status, out id);

			mre.Wait(Database.WorkContext.CancellationToken);

			context.Log(log => log.Debug("\tBulk inserted received {0:#,#;;0} documents in {1}, task #: {2}", documents, sp.Elapsed, id));

			context.WriteJson(new
			{
				OperationId = id
			});
		}

		private static Guid ExtractOperationId(IHttpContext context)
		{
			Guid result;
			Guid.TryParse(context.Request.QueryString["operationId"], out result);
			return result;
		}

		private static IEnumerable<IEnumerable<JsonDocument>> YieldBatches(IHttpContext context, ManualResetEventSlim mre, Action<int> increaseDocumentsCount)
		{
			try
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
							yield return YieldDocumentsInBatch(stream, increaseDocumentsCount);
						}
					}
				}
			}
			finally
			{
				mre.Set();
			}
		}

		private static IEnumerable<JsonDocument> YieldDocumentsInBatch(Stream partialStream, Action<int> increaseDocumentsCount)
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

				increaseDocumentsCount(count);
			}
		}

        public class BulkInsertStatus
        {
            public int Documents { get; set; }
            public bool Completed { get; set; }
        }
	}

}
