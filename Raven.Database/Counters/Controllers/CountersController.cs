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
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.FileSystem;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;

namespace Raven.Database.Counters.Controllers
{
	public class CountersController : RavenCountersApiController
	{
		[RavenRoute("cs/{counterName}/change")]
		[HttpPost]
		public HttpResponseMessage CounterChange(string group, string counterName, long delta)
		{
			using (var writer = Storage.CreateWriter())
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				writer.Store(Storage.CounterStorageUrl, counterFullName, delta);
				writer.Commit(delta != 0);
                Storage.MetricsCounters.ClientRequests.Mark();
				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("cs/{counterName}/batch")]
		[HttpPost]
		public async Task<HttpResponseMessage> CountersBatch()
		{
			/*if (string.IsNullOrEmpty(GetQueryStringValue("no-op")) == false)
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

				var token = authorizer.GenerateSingleUseAuthToken(CountersName, User);
				return GetMessageWithObject(new
				{
					Token = token
				});
			}*/

			if (HttpContext.Current != null)
				HttpContext.Current.Server.ScriptTimeout = 60 * 60 * 6; // six hours should do it, I think.

			//var operationId = ExtractOperationId();
			var sp = Stopwatch.StartNew();

			var status = new BatchStatus {IsTimedOut = false};

			var timeoutTokenSource = new CancellationTokenSource();
			var counterChanges = 0;
			
			var operationId = ExtractOperationId();
			var inputStream = await InnerRequest.Content.ReadAsStreamAsync().ConfigureAwait(false);

			var task = Task.Factory.StartNew(() =>
            {
				
				var timeout = timeoutTokenSource.TimeoutAfter(TimeSpan.FromSeconds(360)); //TODO : make this configurable

				var changeBatches = YieldChangeBatches(inputStream, timeout, countOfChanges => counterChanges += countOfChanges);
	            try
	            {
		            using (var writer = Storage.CreateWriter())
		            {
			            changeBatches.ForEach(batch =>
				            batch.ForEach(change => StoreChange(change, writer)));
			            writer.Commit();
		            }
	            }
	            catch (OperationCanceledException)
	            {
		            // happens on timeout
		            /*DatabasesLandlord.SystemDatabase
						.Notifications.RaiseNotifications(
						new BulkInsertChangeNotification {OperationId = operationId, Message = "Operation cancelled, likely because of a batch timeout", Type = DocumentChangeTypes.BulkInsertError});*/
		            status.IsTimedOut = true;
		            status.Faulted = true;
		            throw;
	            }
	            catch (Exception e)
	            {
		            status.Faulted = true;
		            status.State = RavenJObject.FromObject(new {Error = e.SimplifyException().Message});
		            throw;
	            }
	            finally
	            {
		            status.Completed = true;
		            status.Counters = counterChanges;
	            }
			}, timeoutTokenSource.Token);

			//TODO: do not forget to add task Id
			AddRequestTraceInfo(log => log.AppendFormat("\tCounters batch operation received {0:#,#;;0} changes in {1}", counterChanges, sp.Elapsed));

			long id;
			DatabasesLandlord.SystemDatabase.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.CounterBatchOperation,
				Payload = operationId.ToString()
			}, out id, timeoutTokenSource);

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		private IEnumerable<IEnumerable<CounterChange>> YieldChangeBatches(Stream requestStream, CancellationTimeout timeout, Action<int> changeCounterFunc)
		{
			var serializer = JsonExtensions.CreateDefaultJsonSerializer();
			try
			{
				using (requestStream)
				{
					var binaryReader = new BinaryReader(requestStream);
					while (true)
					{
						timeout.ThrowIfCancellationRequested();
						int batchSize;
						try
						{
							batchSize = binaryReader.ReadInt32();
						}
						catch (EndOfStreamException)
						{
							break;
						}
						using (var stream = new PartialStream(requestStream, batchSize))
						{
							yield return YieldBatchItems(stream, serializer, timeout, changeCounterFunc);
						}
					}
				}
			}
			finally
			{
				requestStream.Close();
			}

		}

		private IEnumerable<CounterChange> YieldBatchItems(Stream partialStream, JsonSerializer serializer, CancellationTimeout timeout, Action<int> changeCounterFunc)
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

					yield return doc.ToObject<CounterChange>(serializer);
				}

				changeCounterFunc(count);
			}
		}

		private void StoreChange(CounterChange counterChange, CounterStorage.Writer writer)
		{
			var counterFullName = String.Join(Constants.GroupSeperatorString, new[] {counterChange.Group, counterChange.Name});
			writer.Store(Storage.CounterStorageUrl, counterFullName, counterChange.Delta);
		}

		public class BatchStatus : IOperationState
		{
			public int Counters { get; set; }
			public bool Completed { get; set; }

			public bool Faulted { get; set; }

			public RavenJToken State { get; set; }

			public bool IsTimedOut { get; set; }
		}

		[RavenRoute("cs/{counterName}/reset")]
		[HttpPost]
		public HttpResponseMessage CounterReset(string group, string counterName)
		{
			using (var writer = Storage.CreateWriter())
			{
				string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				bool changesWereMade = writer.Reset(Storage.CounterStorageUrl, counterFullName);

				if (changesWereMade)
				{
					writer.Commit();
				}
				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("cs/{counterName}/groups")]
		[HttpGet]
		public HttpResponseMessage GetCounterGroups()
		{
			using (var reader = Storage.CreateReader())
			{
				return Request.CreateResponse(HttpStatusCode.OK, reader.GetCounterGroups().ToList());
			}
		}

		[RavenRoute("cs/{counterName}/counters")]
		[HttpGet]
		public HttpResponseMessage GetCounters(int skip = 0, int take = 20, string group = null)
		{
			using (var reader = Storage.CreateReader())
			{
				var prefix = (group == null) ? string.Empty : (group + Constants.GroupSeperatorString);
				var results = (
					from counterFullName in reader.GetCounterNames(prefix)
					let counter = reader.GetCounter(counterFullName)
					select new CounterView
					{
						Name = counterFullName.Split(Constants.GroupSeperatorChar)[1],
						Group = counterFullName.Split(Constants.GroupSeperatorChar)[0],
						OverallTotal = counter.ServerValues.Sum(x => x.Positive - x.Negative),

						Servers = counter.ServerValues.Select(s => new CounterView.ServerValue
						{
							Negative = s.Negative, Positive = s.Positive, Name = reader.ServerNameFor(s.SourceId)
						}).ToList()
					}).ToList();
				return Request.CreateResponse(HttpStatusCode.OK, results);
			}
		}

        [RavenRoute("cs/{counterName}/getCounterOverallTotal")]
        [HttpGet]
		public HttpResponseMessage GetCounterOverallTotal(string group, string counterName)
        {
			using (var reader = Storage.CreateReader())
			{
				var counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
				var counter = reader.GetCounter(counterFullName);
				
				if (counter == null)
				{
					return Request.CreateResponse(HttpStatusCode.NotFound);
				}

				long overallTotal = counter.ServerValues.Sum(x => x.Positive - x.Negative);
				return Request.CreateResponse(HttpStatusCode.OK, overallTotal);
			}
        }

        [RavenRoute("cs/{counterName}/getCounterServersValues")]
        [HttpGet]
        public HttpResponseMessage GetCounterServersValues(string group, string counterName)
        {
            using (var reader = Storage.CreateReader())
            {
                string counterFullName = String.Join(Constants.GroupSeperatorString, new[] { group, counterName });
                Counter counter = reader.GetCounter(counterFullName);

                if (counter == null)
                {
                    return Request.CreateResponse(HttpStatusCode.NotFound);
                }

                List<CounterView.ServerValue> serverValues =
                    counter.ServerValues.Select(s => new CounterView.ServerValue
                    {
                        Negative = s.Negative,
                        Positive = s.Positive,
                        Name = reader.ServerNameFor(s.SourceId)
                    }).ToList();
                return Request.CreateResponse(HttpStatusCode.OK, serverValues);
            }
        }

        [RavenRoute("cs/{counterName}/metrics")]
        [HttpGet]
        public HttpResponseMessage CounterMetrics()
        {
            return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateMetrics());            
        }

        [RavenRoute("cs/{counterName}/stats")]
        [HttpGet]
        public HttpResponseMessage CounterStats()
        {
            return Request.CreateResponse(HttpStatusCode.OK, Storage.CreateStats());
        }
	}
}