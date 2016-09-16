using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
    public class DocumentsBatchController : RavenDbApiController
    {
        [HttpPost]
        [RavenRoute("bulk_docs")]
        [RavenRoute("databases/{databaseName}/bulk_docs")]
        public async Task<HttpResponseMessage> BulkPost()
        {
            using (var cts = new CancellationTokenSource())
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                RavenJArray jsonCommandArray;

                try
                {
                    jsonCommandArray = await ReadJsonArrayAsync();
                }
                catch (InvalidOperationException e)
                {
                    Log.DebugException("Failed to read json documents batch." , e);
                    return GetMessageWithObject(new
                    {
                        Message = "Could not understand json, please check its validity."
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
                    
                }
                catch (InvalidDataException e)
                {
                    Log.DebugException("Failed to read json documents batch.", e);
                    return GetMessageWithObject(new
                    {
                        e.Message
                    }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
                }

                cts.Token.ThrowIfCancellationRequested();

                var transactionInformation = GetRequestTransaction();
                var commands =
                    (from RavenJObject jsonCommand in jsonCommandArray select CommandDataFactory.CreateCommand(jsonCommand, transactionInformation)).ToArray();

                Log.Debug(
                    () =>
                    {
                        if (commands.Length > 15) // this is probably an import method, we will input minimal information, to avoid filling up the log
                        {
                            return "\tExecuted "
                                   + string.Join(
                                       ", ", commands.GroupBy(x => x.Method).Select(x => string.Format("{0:#,#;;0} {1} operations", x.Count(), x.Key)));
                        }

                        var sb = new StringBuilder();
                        foreach (var commandData in commands)
                        {
                            sb.AppendFormat("\t{0} {1}{2}", commandData.Method, commandData.Key, Environment.NewLine);
                        }
                        return sb.ToString();
                    });

                var batchResult = Database.Batch(commands, cts.Token);
                return GetMessageWithObject(batchResult);
            }
        }

        [HttpDelete]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public HttpResponseMessage BulkDelete(string id)
        {
            var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(id);
            if (indexDefinition == null)
                throw new IndexDoesNotExistsException(string.Format("Index '{0}' does not exist.", id));

            if (indexDefinition.IsMapReduce)
                throw new InvalidOperationException("Cannot execute DeleteByIndex operation on Map-Reduce indexes.");

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);
            return OnBulkOperation(databaseBulkOperations.DeleteByIndex, id, timeout);
        }

        [HttpPatch]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public async Task<HttpResponseMessage> BulkPatch(string id)
        {
            RavenJArray patchRequestJson;
            try
            {
                patchRequestJson = await ReadJsonArrayAsync();
            }
            catch (InvalidOperationException e)
            {
                Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity."
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity

            }
            catch (InvalidDataException e)
            {
                Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    e.Message
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);

            var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
            return OnBulkOperation((index, query, options) => databaseBulkOperations.UpdateByIndex(index, query, patchRequests, options), id, timeout);
        }

        [HttpEval]
        [RavenRoute("bulk_docs/{*id}")]
        [RavenRoute("databases/{databaseName}/bulk_docs/{*id}")]
        public async Task<HttpResponseMessage> BulkEval(string id)
        {
            RavenJObject advPatchRequestJson;

            try
            {
                advPatchRequestJson = await ReadJsonObjectAsync<RavenJObject>();
            }
            catch (InvalidOperationException e)
            {
                Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity."
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity

            }
            catch (InvalidDataException e)
            {
                Log.DebugException("Failed to deserialize document batch request." , e);
                return GetMessageWithObject(new
                {
                    e.Message
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            // we don't use using because execution is async
            var cts = new CancellationTokenSource();
            var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout);

            var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);

            var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
            return OnBulkOperation((index, query, options) => databaseBulkOperations.UpdateByIndex(index, query, advPatch, options), id, timeout);
        }

        private HttpResponseMessage OnBulkOperation(Func<string, IndexQuery, BulkOperationOptions, RavenJArray> batchOperation, string index, CancellationTimeout timeout)
        {
            if (string.IsNullOrEmpty(index))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var option = new BulkOperationOptions
            {
                AllowStale = GetAllowStale(),
                MaxOpsPerSec = GetMaxOpsPerSec(),
                StaleTimeout = GetStaleTimeout(),
                RetrieveDetails = GetRetrieveDetails()
            };

            var indexQuery = GetIndexQuery(maxPageSize: int.MaxValue);

            var status = new BulkOperationStatus();
            long id;

            var task = Task.Factory.StartNew(() =>
            {
                using (DocumentCacher.SkipSetDocumentsInDocumentCache())
                {
                    status.State = batchOperation(index, indexQuery, option);
                }
            }).ContinueWith(t =>
            {
                if (timeout != null)
                    timeout.Dispose();

                if (t.IsFaulted == false)
                {
                    status.Completed = true;
                    return;
                }

                var exception = t.Exception.ExtractSingleInnerException();

                status.State = RavenJObject.FromObject(new { Error = exception.Message });
                status.Faulted = true;
                status.Completed = true;
            });

            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
                                                 {
                                                     StartTime = SystemTime.UtcNow,
                                                     TaskType = TaskActions.PendingTaskType.IndexBulkOperation,
                                                     Payload = index
                                                 }, out id, timeout.CancellationTokenSource);

            return GetMessageWithObject(new { OperationId = id });
        }

        public class BulkOperationStatus : IOperationState
        {
            public RavenJToken State { get; set; }
            public bool Completed { get; set; }
            public bool Faulted { get; set; }
        }
    }
}
