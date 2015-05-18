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
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.FileSystem;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util.Streams;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using BatchType = Raven.Abstractions.Counters.Notifications.BatchType;

namespace Raven.Database.Counters.Controllers
{
	public class CounterOperationsController : RavenCountersApiController
	{
		[RavenRoute("cs/{counterStorageName}/change/{groupName}/{counterName}")]
		[HttpPost]
		public HttpResponseMessage CounterChange(string groupName, string counterName, long delta)
		{
			using (var writer = Storage.CreateWriter())
			{
				var counterChangeAction = writer.Store(groupName, counterName, delta);
				writer.Commit(delta != 0);

				Storage.MetricsCounters.ClientRequests.Mark();
				Storage.Publisher.RaiseNotification(new LocalChangeNotification
				{
					GroupName = groupName,
					CounterName = counterName,
					Action = counterChangeAction
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("cs/{counterStorageName}/groups")]
		[HttpGet]
		public HttpResponseMessage GetCounterGroups()
		{
			using (var reader = Storage.CreateReader())
			{
				return Request.CreateResponse(HttpStatusCode.OK, reader.GetCounterGroups().ToList());
			}
		}

		[RavenRoute("cs/{counterStorageName}/batch")]
		[HttpPost]
		public async Task<HttpResponseMessage> CountersBatch()
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

				var token = authorizer.GenerateSingleUseAuthToken(TenantName, User);
				return GetMessageWithObject(new
				{
					Token = token
				});
			}

			if (HttpContext.Current != null)
				HttpContext.Current.Server.ScriptTimeout = 60 * 60 * 6; // six hours should do it, I think.

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
		            foreach (var changeBatch in changeBatches)
		            {
						using (var writer = Storage.CreateWriter())
						{
							Storage.Publisher.RaiseNotification(new BulkOperationNotification
							{
								Type = BatchType.Started,
								OperationId = operationId
							});

							foreach (var counterChange in changeBatch)
							{
								writer.Store(counterChange.Group, counterChange.Name, counterChange.Delta);
							}
							writer.Commit();

							Storage.Publisher.RaiseNotification(new BulkOperationNotification
							{
								Type = BatchType.Ended,
								OperationId = operationId
							});
						}
		            }
	            }
	            catch (OperationCanceledException)
	            {
					// happens on timeout
		            Storage.Publisher.RaiseNotification(new BulkOperationNotification
		            {
			            Type = BatchType.Error,
			            OperationId = operationId,
						Message = "Operation cancelled, likely because of a batch timeout"
		            });
		            
		            status.IsTimedOut = true;
		            status.Faulted = true;
		            throw;
	            }
	            catch (Exception e)
	            {
		            var errorMessage = e.SimplifyException().Message;
					Storage.Publisher.RaiseNotification(new BulkOperationNotification
					{
						Type = BatchType.Error,
						OperationId = operationId,
						Message = errorMessage
					});

		            status.Faulted = true;
		            status.State = RavenJObject.FromObject(new {Error = errorMessage});
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

			task.Wait(timeoutTokenSource.Token);

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

		public class BatchStatus : IOperationState
		{
			public int Counters { get; set; }
			public bool Completed { get; set; }

			public bool Faulted { get; set; }

			public RavenJToken State { get; set; }

			public bool IsTimedOut { get; set; }
		}

		[RavenRoute("cs/{counterStorageName}/reset/{groupName}/{counterName}")]
		[HttpPost]
		public HttpResponseMessage CounterReset(string groupName, string counterName)
		{
			using (var writer = Storage.CreateWriter())
			{
				var counterChangeAction = writer.Reset(groupName, counterName);

				if (counterChangeAction != CounterChangeAction.None)
				{
					writer.Commit();

					Storage.MetricsCounters.Resets.Mark();
					Storage.Publisher.RaiseNotification(new LocalChangeNotification
					{
						GroupName = groupName,
						CounterName = counterName,
						Action = counterChangeAction,
					});
				}

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("cs/{counterStorageName}/counters")]
		[HttpGet]
		public HttpResponseMessage GetCounters(int skip = 0, int take = 20, string inputGroupName = null)
		{
			using (var reader = Storage.CreateReader())
			{
				//TODO: implement
				/*var prefix = (inputGroupName == null) ? string.Empty : (inputGroupName + Constants.CountersSeperator);
				//todo: get only the counter prefixes: foo/bar/
				var results = (
					from fullCounterName in reader.GetFullCounterNames(prefix)
					let groupName = fullCounterName.Split(Constants.CountersSeperator)[0]
					let counterName = fullCounterName.Split(Constants.CountersSeperator)[1]
					let counter = reader.GetCounterValuesByPrefix(groupName, counterName)
					/*let overallTotalPositive = counter.CounterValues.Where(x => x.IsPositive).Sum(x => x.Value)
					let overallTotalNegative = counter.CounterValues.Where(x => !x.IsPositive).Sum(x => x.Value)#1#
					select new CounterView
					{
						Name = counterName,
						Group = groupName,
						OverallTotal = CounterStorage.CalculateOverallTotal(counter),
						Servers = (from counterValue in counter.CounterValues
								  group counterValue by counterValue.ServerId into g
								  select new CounterView.ServerValue{
									  Name = g.Select(x => x.ServerName).ToString(),
									  Positive = g.Where(x => x.IsPositive).Select(x => x.Value).FirstOrDefault(),
									  Negative = g.Where(x => !x.IsPositive).Select(x => x.Value).FirstOrDefault(),
								  }).ToList()
						/*Servers = counter.CounterValues.Select(s => new CounterView.ServerValue
						{
							Negative = s.Negative, 
							Positive = s.Positive, 
							Name = CounterStorage.Reader.ServerNameFor(s.SourceId)
						}).ToList()#1#
					}).ToList();
				return Request.CreateResponse(HttpStatusCode.OK, results);*/
				return Request.CreateResponse(HttpStatusCode.OK);
			}
		}

		[RavenRoute("cs/{counterStorageName}/getCounterOverallTotal/{groupName}/{counterName}")]
        [HttpGet]
		public HttpResponseMessage GetCounterOverallTotal(string groupName, string counterName)
        {
			using (var reader = Storage.CreateReader())
			{
				var overallTotal = reader.GetCounterOverallTotal(groupName, counterName);
				if (overallTotal == null)
					return Request.CreateResponse(HttpStatusCode.OK, 0);

				return Request.CreateResponse(HttpStatusCode.OK, overallTotal);
			}
        }

		[RavenRoute("cs/{counterStorageName}/getCounterServersValues/{groupName}/{counterName}")]
        [HttpGet]
        public HttpResponseMessage GetCounterServersValues(string groupName, string counterName)
		{				
			using (var reader = Storage.CreateReader())
			{
				if (reader.CounterExists(groupName, counterName) == false)
					return Request.CreateResponse(HttpStatusCode.OK, new ServerValue[0]);

				var countersByPrefix = reader.GetCounterValuesByPrefix(groupName, counterName);
                if (countersByPrefix == null)
				{
					return GetMessageWithObject(new { Message = "Specified counter not found within the specified group" }, HttpStatusCode.NotFound);
                }

	            var serverValuesDictionary = new Dictionary<Guid, ServerValue>();
				countersByPrefix.CounterValues.ForEach(x =>
				{
					ServerValue serverValue;
					if (serverValuesDictionary.TryGetValue(x.ServerId, out serverValue) == false)
					{
						serverValue = new ServerValue();
						serverValuesDictionary.Add(x.ServerId, serverValue);
					}
					serverValue.UpdateValue(x.IsPositive, x.Value);
				});

                var serverValues =
                    serverValuesDictionary.Select(s => new CounterView.ServerValue
                    {
						Positive = s.Value.Positive,
                        Negative = s.Value.Negative,
                        //Name = reader.ServerNameFor(s.Key)
                    }).ToList();
                return Request.CreateResponse(HttpStatusCode.OK, serverValues);
            }
		}

		private class ServerValue
		{
			public long Positive { get; private set; }

			public long Negative { get; private set; }

			public void UpdateValue(bool isPositive, long value)
			{
				if (isPositive)
				{
					Positive = value;
				}
				else
				{
					Negative = value;
				}
			}
		}
	}
}