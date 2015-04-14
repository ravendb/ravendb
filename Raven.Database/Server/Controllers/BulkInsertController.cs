using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Mono.Unix.Native;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;

using Raven.Client.FileSystem;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
    public class BulkInsertController : RavenDbApiController
    {
        [HttpPost]
        [RavenRoute("bulkInsert")]
        [RavenRoute("databases/{databaseName}/bulkInsert")]
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

                var token = authorizer.GenerateSingleUseAuthToken(DatabaseName, User);
                return GetMessageWithObject(new
                {
                    Token = token
                });
            }

            if (HttpContext.Current != null)
                HttpContext.Current.Server.ScriptTimeout = 60 * 60 * 6; // six hours should do it, I think.

            var options = new BulkInsertOptions
            {
                OverwriteExisting = GetOverwriteExisting(),
                CheckReferencesInIndexes = GetCheckReferencesInIndexes(),
				SkipOverwriteIfUnchanged = GetSkipOverwriteIfUnchanged()
            };

            var operationId = ExtractOperationId();
            var sp = Stopwatch.StartNew();

            var status = new BulkInsertStatus();
            status.IsTimedOut = false;

            var documents = 0;
            var mre = new ManualResetEventSlim(false);
            var tre = new CancellationTokenSource();
            
            var inputStream = await InnerRequest.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var currentDatabase = Database;
            var timeout = tre.TimeoutAfter(currentDatabase.Configuration.BulkImportBatchTimeout);
            var user = CurrentOperationContext.User.Value;
            var headers = CurrentOperationContext.Headers.Value;
            Exception error = null;
            var task = Task.Factory.StartNew(() =>
            {
                try
                {
                    CurrentOperationContext.User.Value = user;
                    CurrentOperationContext.Headers.Value = headers;
                    currentDatabase.Documents.BulkInsert(options, YieldBatches(timeout, inputStream, mre, batchSize => documents += batchSize), operationId, tre.Token);
                }
				catch (InvalidDataException e)
				{
					status.Faulted = true;
					status.State = RavenJObject.FromObject(new { Error = "Could not understand json.", InnerError = e.SimplifyException().Message });
					status.IsSerializationError = true;
					error = e;
				}
				catch (OperationCanceledException)
                {
                    // happens on timeout
                    currentDatabase.Notifications.RaiseNotifications(new BulkInsertChangeNotification { OperationId = operationId, Message = "Operation cancelled, likely because of a batch timeout", Type = DocumentChangeTypes.BulkInsertError });
                    status.IsTimedOut = true;
                    status.Faulted = true;
                }
                catch (Exception e)
                {
                    status.Faulted = true;
                    status.State = RavenJObject.FromObject(new { Error = e.SimplifyException().Message });
                    error = e;
                }
                finally
                {
                    status.Completed = true;
                    status.Documents = documents;
	                CurrentOperationContext.User.Value = null;
	                CurrentOperationContext.Headers.Value = null;
                }
			}, tre.Token);

            long id;
            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
                                                 {
                                                     StartTime = SystemTime.UtcNow,
                                                     TaskType = TaskActions.PendingTaskType.BulkInsert,
                                                     Payload = operationId.ToString()
                                                 }, out id, tre);

            await task;

            if (error != null)
            {
				var httpStatusCode = status.IsSerializationError ? (HttpStatusCode)422 : HttpStatusCode.InternalServerError;
	            return GetMessageWithObject(new
                {
                    error.Message,
                    Error = error.ToString()
				}, httpStatusCode);
            }
	        if (status.IsTimedOut)
                throw new TimeoutException("Bulk insert operation did not receive new data longer than configured treshold");

            sp.Stop();

            AddRequestTraceInfo(log => log.AppendFormat("\tBulk inserted received {0:#,#;;0} documents in {1}, task #: {2}", documents, sp.Elapsed, id));

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

        private IEnumerable<IEnumerable<JsonDocument>> YieldBatches(CancellationTimeout timeout, Stream inputStream, ManualResetEventSlim mre, Action<int> increaseDocumentsCount)
        {
            try
            {
                using (inputStream)
                {
                    var binaryReader = new BinaryReader(inputStream);

                    while (true)
                    {
                        timeout.ThrowIfCancellationRequested();
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
                            yield return YieldDocumentsInBatch(timeout, stream, increaseDocumentsCount);
                        }
                    }
                }
            }
            finally
            {
                mre.Set();
                inputStream.Close();
            }
        }

        private IEnumerable<JsonDocument> YieldDocumentsInBatch(CancellationTimeout timeout, Stream partialStream, Action<int> increaseDocumentsCount)
        {
            using (var stream = new GZipStream(partialStream, CompressionMode.Decompress, leaveOpen: true))
            {
                var reader = new BinaryReader(stream);
                var count = reader.ReadInt32();

                for (var i = 0; i < count; i++)
                {
                    timeout.Delay();
                    var doc = (RavenJObject)RavenJToken.ReadFrom(new BsonReader(reader)
                                                                 {
                                                                     DateTimeKindHandling = DateTimeKind.Unspecified
                                                                 });

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

        public class BulkInsertStatus : IOperationState
        {
            public int Documents { get; set; }
            public bool Completed { get; set; }

            public bool Faulted { get; set; }

            public RavenJToken State { get; set; } 

            public bool IsTimedOut { get; set; }

			public bool IsSerializationError { get; set; }
        }
    }
}