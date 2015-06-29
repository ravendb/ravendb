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
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.TimeSeries.Notifications;
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
using BatchType = Raven.Abstractions.TimeSeries.Notifications.BatchType;

namespace Raven.Database.TimeSeries.Controllers
{
	public class TimeSeriesOperationsController : RavenTimeSeriesApiController
	{
		[RavenRoute("ts/{timeSeriesName}/append/{key}")]
		[HttpPost]
		public HttpResponseMessage Append(string key, long time, double[] values)
		{
			if (string.IsNullOrEmpty(key))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			using (var writer = Storage.CreateWriter())
			{
				writer.Append(key, new DateTime(time), values);
				writer.Commit();

				Storage.MetricsTimeSeries.ClientRequests.Mark();
				Storage.Publisher.RaiseNotification(new TimeSeriesKeyNotification
				{
					Key = key,
					Action = TimeSeriesChangeAction.Append,
					At = time,
					Values = values,
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/batch")]
		[HttpPost]
		public async Task<HttpResponseMessage> TimeSeriesBatch()
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
			var timeSeriesChanges = 0;
			
			var operationId = ExtractOperationId();
			var inputStream = await InnerRequest.Content.ReadAsStreamAsync().ConfigureAwait(false);
			var task = Task.Factory.StartNew(() =>
            {
				var timeout = timeoutTokenSource.TimeoutAfter(TimeSpan.FromSeconds(360)); //TODO : make this configurable

				var changeBatches = YieldChangeBatches(inputStream, timeout, countOfChanges => timeSeriesChanges += countOfChanges);
	            try
	            {
		            foreach (var changeBatch in changeBatches)
		            {
						using (var writer = Storage.CreateWriter())
						{
							Storage.Publisher.RaiseNotification(new TimeSeriesBulkOperationNotification
							{
								Type = BatchType.Started,
								OperationId = operationId
							});

							foreach (var change in changeBatch)
							{
								if (string.IsNullOrWhiteSpace(change.Key))
									throw new InvalidOperationException("Key cannot be empty");
								writer.Append(change.Key, change.At, change.Values);
							}
							writer.Commit();

							Storage.Publisher.RaiseNotification(new TimeSeriesBulkOperationNotification
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
		            Storage.Publisher.RaiseNotification(new TimeSeriesBulkOperationNotification
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
					Storage.Publisher.RaiseNotification(new TimeSeriesBulkOperationNotification
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
		            status.TimeSeries = timeSeriesChanges;
	            }
			}, timeoutTokenSource.Token);

			//TODO: do not forget to add task Id
			AddRequestTraceInfo(log => log.AppendFormat("\tTimeSeries batch operation received {0:#,#;;0} changes in {1}", timeSeriesChanges, sp.Elapsed));

			long id;
			DatabasesLandlord.SystemDatabase.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.TimeSeriesBatchOperation,
				Payload = operationId.ToString()
			}, out id, timeoutTokenSource);

			task.Wait(timeoutTokenSource.Token);

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		private IEnumerable<IEnumerable<TimeSeriesAppend>> YieldChangeBatches(Stream requestStream, CancellationTimeout timeout, Action<int> changeTimeSeriesFunc)
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
							yield return YieldBatchItems(stream, serializer, timeout, changeTimeSeriesFunc);
						}
					}
				}
			}
			finally
			{
				requestStream.Close();
			}

		}

		private IEnumerable<TimeSeriesAppend> YieldBatchItems(Stream partialStream, JsonSerializer serializer, CancellationTimeout timeout, Action<int> changeTimeSeriesFunc)
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

					yield return doc.ToObject<TimeSeriesAppend>(serializer);
				}

				changeTimeSeriesFunc(count);
			}
		}

		private class BatchStatus : IOperationState
		{
			public int TimeSeries { get; set; }
			public bool Completed { get; set; }

			public bool Faulted { get; set; }

			public RavenJToken State { get; set; }

			public bool IsTimedOut { get; set; }
		}

		[RavenRoute("ts/{timeSeriesName}/delete")]
		[HttpDelete]
		public HttpResponseMessage Delete(string key)
		{
			if (string.IsNullOrEmpty(key))
				return GetEmptyMessage(HttpStatusCode.BadRequest);
			
			using (var writer = Storage.CreateWriter())
			{
				writer.Delete(key);
				writer.Commit();

				Storage.MetricsTimeSeries.Deletes.Mark();
				Storage.Publisher.RaiseNotification(new TimeSeriesKeyNotification
				{
					Key = key,
					Action = TimeSeriesChangeAction.Delete,
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/deleteRange")]
		[HttpDelete]
		public HttpResponseMessage DeleteRange(string key, DateTime start, DateTime end)
		{
			if (string.IsNullOrEmpty(key))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			using (var writer = Storage.CreateWriter())
			{
				writer.DeleteRange(key, start, end);
				writer.Commit();

				Storage.MetricsTimeSeries.Deletes.Mark();
				Storage.Publisher.RaiseNotification(new TimeSeriesKeyNotification
				{
					Key = key,
					Action = TimeSeriesChangeAction.DeleteInRange,
					Start = start,
					End = end,
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		/*[RavenRoute("ts/{timeSeriesName}/timeSeries")]
		[HttpGet]
		public HttpResponseMessage GetTimeSeries(int skip = 0, int take = 20, string key = null)
		{
			using (var reader = Storage.CreateReader())
			{
				var groupsPrefix = (group == null) ? string.Empty : (group + Constants.TimeSeries.Separator);
				var timeSeriesByPrefixes = reader.GetTimeSeriesByPrefixes(groupsPrefix, skip, take);
				var timeSeries = timeSeriesByPrefixes.Select(groupWithTimeSeriesName => reader.GetTimeSeriesSummary(groupWithTimeSeriesName)).ToList();
				return GetMessageWithObject(timeSeries);
			}
		}*/
	}
}