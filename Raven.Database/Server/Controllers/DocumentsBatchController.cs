using System;
using System.Diagnostics;
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
		[Route("bulk_docs")]
		[Route("databases/{databaseName}/bulk_docs")]
		public async Task<HttpResponseMessage> BulkPost()
		{
		    var cts = new CancellationTokenSource();
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                var jsonCommandArray = await ReadJsonArrayAsync();

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
		[Route("bulk_docs/{*id}")]
		[Route("databases/{databaseName}/bulk_docs/{*id}")]
		public HttpResponseMessage BulkDelete(string id)
		{
		    var cts = new CancellationTokenSource();
            using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
	            var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(id);
				if (indexDefinition == null)
					throw new IndexDoesNotExistsException(string.Format("Index '{0}' does not exist.", id));

				if (indexDefinition.IsMapReduce)
					throw new InvalidOperationException("Cannot execute DeleteByIndex operation on Map-Reduce indexes.");


                var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);
                return OnBulkOperation(databaseBulkOperations.DeleteByIndex, id, cts);
            }
		}

		[HttpPatch]
		[Route("bulk_docs/{*id}")]
		[Route("databases/{databaseName}/bulk_docs/{*id}")]
		public async Task<HttpResponseMessage> BulkPatch(string id)
		{
		    var cts = new CancellationTokenSource();
            using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);
                var patchRequestJson = await ReadJsonArrayAsync();
                var patchRequests = patchRequestJson.Cast<RavenJObject>().Select(PatchRequest.FromJson).ToArray();
                return OnBulkOperation((index, query, allowStale) => databaseBulkOperations.UpdateByIndex(index, query, patchRequests, allowStale), id, cts);
            }
		}

		[HttpEval]
		[Route("bulk_docs/{*id}")]
		[Route("databases/{databaseName}/bulk_docs/{*id}")]
		public async Task<HttpResponseMessage> BulkEval(string id)
		{
		    var cts = new CancellationTokenSource();
            using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                var databaseBulkOperations = new DatabaseBulkOperations(Database, GetRequestTransaction(), cts, timeout);
                var advPatchRequestJson = await ReadJsonObjectAsync<RavenJObject>();
                var advPatch = ScriptedPatchRequest.FromJson(advPatchRequestJson);
                return OnBulkOperation((index, query, allowStale) => databaseBulkOperations.UpdateByIndex(index, query, advPatch, allowStale), id, cts);
            }
		}

		private HttpResponseMessage OnBulkOperation(Func<string, IndexQuery, bool, RavenJArray> batchOperation, string index, CancellationTokenSource cts)
		{
			if (string.IsNullOrEmpty(index))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			var allowStale = GetAllowStale();
			var indexQuery = GetIndexQuery(maxPageSize: int.MaxValue);

			var status = new BulkOperationStatus();
			var sp = Stopwatch.StartNew();
			long id = 0;

			var task = Task.Factory.StartNew(() =>
			{
			    try
			    {

			        var array = batchOperation(index, indexQuery, allowStale);
			        status.State = array;
			        status.Completed = true;
			    }
			    finally
			    {
			        cts.Dispose();
			    }

			    //TODO: log
				//context.Log(log => log.Debug("\tBatch Operation worked on {0:#,#;;0} documents in {1}, task #: {2}", array.Length, sp.Elapsed, id));
			});

			Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
			                                     {
			                                         StartTime = SystemTime.UtcNow,
                                                     TaskType = TaskActions.PendingTaskType.IndexBulkOperation,
                                                     Payload = index
                                                 }, out id, cts);

			return GetMessageWithObject(new { OperationId = id });
		}

		public class BulkOperationStatus
		{
			public RavenJArray State { get; set; }
			public bool Completed { get; set; }
		}
	}
}