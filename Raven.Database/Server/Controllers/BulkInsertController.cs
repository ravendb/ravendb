using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Server.Security;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	[RoutePrefix("")]
	public class BulkInsertController : RavenApiController
	{
		[HttpPost]
		[Route("bulkInsert")]
		[Route("databases/{databaseName}/bulkInsert")]
		public async Task<HttpResponseMessage> BulkInsertPost()
		{
			if (string.IsNullOrEmpty(GetQueryStringValue("no-op")) == false)
			{
				// this is a no-op request which is there just to force the client HTTP layer to handle the authentication
				// only used for legacy clients
				return GetEmptyMessage();
			}
			if ("generate-single-use-auth-token".Equals(GetQueryStringValue("op"), StringComparison.InvariantCultureIgnoreCase))
			{
				// using windows auth with anonymous access = none sometimes generate a 401 even though we made two requests
				// instead of relying on windows auth, which require request buffering, we generate a one time token and return it.
				// we KNOW that the user have access to this db for writing, since they got here, so there is no issue in generating 
				// a single use token for them.

				var authorizer = (MixedModeRequestAuthorizer)Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

				var token = authorizer.GenerateSingleUseAuthToken(Database, User, this);
				return GetMessageWithObject(new
				{
					Token = token
				});
			}

			if (HttpContext.Current != null)
				HttpContext.Current.Server.ScriptTimeout = 60*60*6; // six hours should do it, I think.

			var options = new BulkInsertOptions
			{
				CheckForUpdates = GetCheckForUpdates(),
				CheckReferencesInIndexes = GetCheckReferencesInIndexes()
			};

			var operationId = ExtractOperationId();
			var sp = Stopwatch.StartNew();

			var status = new BulkInsertStatus();

			var documents = 0;
			var mre = new ManualResetEventSlim(false);

			var inputStream = await InnerRequest.Content.ReadAsStreamAsync();
			var currentDatabase = Database;
			var task = Task.Factory.StartNew(() =>
			{
				currentDatabase.BulkInsert(options, YieldBatches(inputStream , mre, batchSize => documents += batchSize), operationId);
				status.Documents = documents;
				status.Completed = true;
			});

			long id;
			Database.AddTask(task, status, out id);

			mre.Wait(Database.WorkContext.CancellationToken);

			//TODO: log
			//context.Log(log => log.Debug("\tBulk inserted received {0:#,#;;0} documents in {1}, task #: {2}", documents, sp.Elapsed, id));

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		private Guid ExtractOperationId()
		{
			Guid result;
			Guid.TryParse(GetQueryStringValue("operationId"), out result);
			return result;
		}

		private IEnumerable<IEnumerable<JsonDocument>> YieldBatches(Stream inputStream,ManualResetEventSlim mre, Action<int> increaseDocumentsCount)
		{
			try
			{
				using (inputStream)
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
			using (var stream = new GZipStream(partialStream, CompressionMode.Decompress, leaveOpen: true))
			{
				var reader = new BinaryReader(stream);
				var count = reader.ReadInt32();

				for (var i = 0; i < count; i++)
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