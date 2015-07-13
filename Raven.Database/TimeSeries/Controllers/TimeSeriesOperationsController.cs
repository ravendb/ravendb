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
		[RavenRoute("ts/{timeSeriesName}/prefix-create/{prefix}")]
		[HttpPost]
		public HttpResponseMessage CreatePrefixConfiguration(string prefix, byte valueLength)
		{
			if (string.IsNullOrEmpty(prefix) || valueLength < 1)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (prefix.StartsWith("-") == false)
				return GetMessageWithString("Prefix must start with '-' char", HttpStatusCode.BadRequest);

			Storage.CreatePrefixConfiguration(prefix, valueLength);
			Storage.MetricsTimeSeries.ClientRequests.Mark();

			return new HttpResponseMessage(HttpStatusCode.Created);
		}

		[RavenRoute("ts/{timeSeriesName}/prefix-delete/{prefix}")]
		[HttpDelete]
		public HttpResponseMessage DeletePrefixConfiguration(string prefix)
		{
			if (string.IsNullOrEmpty(prefix))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (prefix.StartsWith("-") == false)
				return GetMessageWithString("Prefix must start with '-' char", HttpStatusCode.BadRequest);

			Storage.DeletePrefixConfiguration(prefix);
			Storage.MetricsTimeSeries.ClientRequests.Mark();

			return new HttpResponseMessage(HttpStatusCode.Created);
		}

		[RavenRoute("ts/{timeSeriesName}/append/{prefix}/{key}")]
		[HttpPost]
		public HttpResponseMessage Append(string prefix, string key, TimeSeriesPoint input)
		{
			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key) || input.Values == null || input.Values.Length == 0)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (prefix.StartsWith("-") == false)
				throw new InvalidOperationException("Prefix must start with '-' char");

			using (var writer = Storage.CreateWriter())
			{
				writer.Append(prefix, key, input.At, input.Values);
				writer.Commit();

				Storage.MetricsTimeSeries.ClientRequests.Mark();
				Storage.Publisher.RaiseNotification(new KeyChangeNotification
				{
					Prefix = prefix,
					Key = key,
					Action = TimeSeriesChangeAction.Append,
					At = input.At,
					Values = input.Values,
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
							Storage.Publisher.RaiseNotification(new BulkOperationNotification
							{
								Type = BatchType.Started,
								OperationId = operationId
							});

							foreach (var change in changeBatch)
							{
								writer.Append(change.Prefix, change.Key, change.At, change.Values);
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

		[RavenRoute("ts/{timeSeriesName}/delete/{prefix}/{key}")]
		[HttpDelete]
		public HttpResponseMessage Delete(string prefix, string key)
		{
			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key))
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (prefix.StartsWith("-") == false)
				return GetMessageWithString("Prefix must start with '-' char", HttpStatusCode.BadRequest);

			var valueLength = Storage.GetPrefixConfiguration(prefix);
			if (valueLength == 0)
				return GetMessageWithString("Cannot delete from not exist prefix: " + prefix, HttpStatusCode.BadRequest);
			
			using (var writer = Storage.CreateWriter())
			{
				writer.Delete(prefix, key);
				writer.DeleteKeyInRollups(prefix, key);
				writer.Commit();

				Storage.MetricsTimeSeries.Deletes.Mark();
				Storage.Publisher.RaiseNotification(new KeyChangeNotification
				{
					Prefix = prefix,
					Key = key,
					Action = TimeSeriesChangeAction.Delete,
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/deleteRange/{prefix}/{key}")]
		[HttpDelete]
		public HttpResponseMessage DeleteRange(string prefix, string key, long start, long end)
		{
			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key) || start < DateTime.MinValue.Ticks || start > DateTime.MaxValue.Ticks)
				return GetEmptyMessage(HttpStatusCode.BadRequest);

			if (start > end)
				throw new InvalidOperationException("start cannot be greater than end");

			if (prefix.StartsWith("-") == false)
				throw new InvalidOperationException("Prefix must start with '-' char");

			var valueLength = Storage.GetPrefixConfiguration(prefix);
			if (valueLength == 0)
				return GetMessageWithString("Cannot delete from not exist prefix: " + prefix, HttpStatusCode.BadRequest); 
			
			using (var writer = Storage.CreateWriter())
			{
				writer.DeleteRange(prefix, key, start, end);
				writer.DeleteRangeInRollups(prefix, key, start, end);
				writer.Commit();

				Storage.MetricsTimeSeries.Deletes.Mark();
				Storage.Publisher.RaiseNotification(new KeyChangeNotification
				{
					Prefix = prefix,
					Key = key,
					Action = TimeSeriesChangeAction.DeleteInRange,
					Start = start,
					End = end,
				});

				return new HttpResponseMessage(HttpStatusCode.OK);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/keys")]
		[HttpGet]
		public HttpResponseMessage GetKeys()
		{
			using (var reader = Storage.CreateReader())
			{
				Storage.MetricsTimeSeries.ClientRequests.Mark();
				var keys = reader.GetKeys().ToArray();
				return Request.CreateResponse(HttpStatusCode.OK, keys);
			}
		}

		[RavenRoute("ts/{timeSeriesName}/{prefix}/{key}/points")]
		[HttpGet]
		public HttpResponseMessage GetPoints(string prefix, string key, int skip = 0, int take = 20)
		{
			if (skip < 0)
				throw new ArgumentException("Bad argument", "skip");
			if (take <= 0)
				throw new ArgumentException("Bad argument", "take");

			Storage.MetricsTimeSeries.ClientRequests.Mark();
			using (var reader = Storage.CreateReader())
			{
				var points = reader.GetPoints(prefix, key, skip).Take(take);
				return GetMessageWithObject(points);
			}
		}
	}
}