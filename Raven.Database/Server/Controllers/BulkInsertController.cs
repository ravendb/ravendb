using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Unix.Native;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;

using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
    [RoutePrefix("")]
    public class BulkInsertController : BaseDatabaseApiController
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
                SkipOverwriteIfUnchanged = GetSkipOverwriteIfUnchanged(),
                Format = GetFormat(),
                Compression = GetCompression(),
            };

            var operationId = ExtractOperationId();
            var sp = Stopwatch.StartNew();

            var status = new BulkInsertStatus();

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
                    currentDatabase.Documents.BulkInsert(options, YieldBatches(timeout, inputStream, mre, options, batchSize =>
                    {
                        documents += batchSize;
                        status.MarkProgress($"Imported {documents} documents");
                    }), operationId, tre.Token, timeout);
                    status.MarkCompleted($"Imported {documents} documents");
                }
                catch (InvalidDataException e)
                {
                    status.MarkFaulted("Could not understand json:" + e.SimplifyException().Message);
                    status.IsSerializationError = true;
                    error = e;
                }
                catch (OperationCanceledException)
                {
                    // happens on timeout
                    currentDatabase.Notifications.RaiseNotifications(new BulkInsertChangeNotification { OperationId = operationId, Message = "Operation cancelled, likely because of a batch timeout", Type = DocumentChangeTypes.BulkInsertError });
                    status.MarkCanceled("Operation cancelled, likely because of a batch timeout");
                }
                catch (OperationVetoedException e)
                {
                    status.MarkFaulted();
                    error = e;
                }
                catch (Exception e)
                {
                    status.MarkFaulted(e.SimplifyException().Message);
                    error = e;
                }
                finally
                {
                    status.Documents = documents;
                    CurrentOperationContext.User.Value = null;
                    CurrentOperationContext.Headers.Value = null;
                    timeout.Dispose();
                }
            }, tre.Token);

            long id;
            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
                                                 {
                                                     StartTime = SystemTime.UtcNow,
                                                     TaskType = TaskActions.PendingTaskType.BulkInsert,
                                                     Description = operationId.ToString()
                                                 }, out id, tre);

            await task.ConfigureAwait(false);

            if (error != null)
            {
                var httpStatusCode = status.IsSerializationError ? (HttpStatusCode)422 : HttpStatusCode.InternalServerError;
                return GetMessageWithObject(new
                {
                    error.Message,
                    Error = error.ToString()
                }, httpStatusCode);
            }
            if (status.Canceled)
                throw new TimeoutException("Bulk insert operation did not receive new data longer than configured threshold");

            sp.Stop();

            AddRequestTraceInfo(log => log.AppendFormat("\tBulk inserted received {0:#,#;;0} documents in {1}, task #: {2}", documents, sp.Elapsed, id));

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        private IEnumerable<IEnumerable<JsonDocument>> YieldBatches(CancellationTimeout timeout, Stream inputStream, ManualResetEventSlim mre, BulkInsertOptions options, Action<int> increaseDocumentsCount)
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
                            yield return YieldDeserializeDocumentsInBatch(timeout, stream, options, increaseDocumentsCount);
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

        private IEnumerable<JsonDocument> YieldDeserializeDocumentsInBatch(CancellationTimeout timeout, Stream partialStream, BulkInsertOptions options, Action<int> increaseDocumentsCount)
        {
            switch (options.Compression)
            {
                case BulkInsertCompression.GZip:
                    {
                        using (var gzip = new GZipStream(partialStream, CompressionMode.Decompress, leaveOpen: true))
                        {
                            return YieldDocumentsInBatch(timeout, gzip, options, increaseDocumentsCount);
                        }
                    }
                case BulkInsertCompression.None:
                    {
                        return YieldDocumentsInBatch(timeout, partialStream, options, increaseDocumentsCount);
                    }
                default: throw new NotSupportedException(string.Format("The compression algorithm '{0}' is not supported", options.Compression.ToString()));
            }
        }

        private IEnumerable<JsonDocument> YieldDocumentsInBatch(CancellationTimeout timeout, Stream partialStream, BulkInsertOptions options, Action<int> increaseDocumentsCount)
        {
            using (var reader = new BinaryReader(partialStream))
            {
                switch (options.Format)
                {
                    case BulkInsertFormat.Bson:
                        {
                var count = reader.ReadInt32();

                            return YieldBsonDocumentsInBatch(timeout, reader, count, increaseDocumentsCount).ToArray();
                        }
                    case BulkInsertFormat.Json:
                        {
                            var count = reader.ReadInt32();

                            return YieldJsonDocumentsInBatch(timeout, partialStream, count, increaseDocumentsCount).ToArray();
                        }
                    default: throw new NotSupportedException(string.Format("The format '{0}' is not supported", options.Format.ToString()));
                }
            }
        }

        private IEnumerable<JsonDocument> YieldBsonDocumentsInBatch(CancellationTimeout timeout, BinaryReader reader, int count, Action<int> increaseDocumentsCount)
        {
            using (var jsonReader = new BsonReader(reader) { SupportMultipleContent = true, DateTimeKindHandling = DateTimeKind.Unspecified })
            {
                for (var i = 0; i < count; i++)
                {
                    timeout.Delay();

                    while (jsonReader.Read())
                                                                 {
                        if (jsonReader.TokenType == JsonToken.StartObject)
                            break;
                    }

                    if (jsonReader.TokenType != JsonToken.StartObject)
                        throw new InvalidOperationException("Could not get document");

                    var doc = (RavenJObject)RavenJToken.ReadFrom(jsonReader);

                    var jsonDocument = PrepareJsonDocument(doc);
                    if (jsonDocument == null)
                        continue;

                    yield return jsonDocument;
                }

                increaseDocumentsCount(count);
            }
        }

        private IEnumerable<JsonDocument> YieldJsonDocumentsInBatch(CancellationTimeout timeout, Stream stream, int count, Action<int> increaseDocumentsCount)
        {
            using (JsonTextReader jsonReader = new JsonTextReader(new StreamReader(stream)) { SupportMultipleContent = true })
            {
                for (var i = 0; i < count; i++)
                {
                    timeout.Delay();

                    while (jsonReader.Read())
                    {
                        if (jsonReader.TokenType == JsonToken.StartObject)
                            break;
                    }

                    if (jsonReader.TokenType != JsonToken.StartObject)
                        throw new InvalidOperationException("Could not get document");

                    var doc = (RavenJObject)RavenJToken.ReadFrom(jsonReader);

                    var jsonDocument = PrepareJsonDocument(doc);
                    if (jsonDocument == null)
                        continue;

                    yield return jsonDocument;
                }

                increaseDocumentsCount(count);
            }
        }

        private static JsonDocument PrepareJsonDocument(RavenJObject doc)
        {
            var metadata = doc.Value<RavenJObject>("@metadata");
            if (metadata == null)
                throw new InvalidOperationException("Could not find metadata for document");

            var id = metadata.Value<string>("@id");
            if (string.IsNullOrEmpty(id))
                throw new InvalidOperationException("Could not get id from metadata");

            if (id.Equals(Constants.BulkImportHeartbeatDocKey, StringComparison.InvariantCultureIgnoreCase))
                return null; //its just a token document, should not get written into the database
                             //the purpose of the heartbeat document is to make sure that the connection doesn't time-out
                             //during long pauses in the bulk insert operation.
                             // Currently used by smuggler to make sure that the connection doesn't time out if there is a 
                             //continuation token and lots of document skips

            doc.Remove("@metadata");

            return new JsonDocument
            {
                Key = id,
                DataAsJson = doc,
                Metadata = metadata
            };
        }

        public class BulkInsertStatus : OperationStateBase
        {
            public int Documents { get; set; }

            public bool IsSerializationError { get; set; }
        }
    }
}
