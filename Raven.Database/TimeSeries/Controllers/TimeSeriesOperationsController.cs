using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
using Raven.Client.FileSystem.Extensions;
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
    public class TimeSeriesOperationsController : BaseTimeSeriesApiController
    {
        [RavenRoute("ts/{timeSeriesName}/types/{type}")]
        [HttpPut]
        public HttpResponseMessage PutType(TimeSeriesType type)
        {
            if (string.IsNullOrEmpty(type.Type) || type.Fields == null || type.Fields.Length < 1)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            using (var writer = TimeSeries.CreateWriter())
            {
                writer.CreateType(type.Type, type.Fields);
                writer.Commit();
            }

            TimeSeries.MetricsTimeSeries.ClientRequests.Mark();

            return new HttpResponseMessage(HttpStatusCode.Created);
        }

        [RavenRoute("ts/{timeSeriesName}/types/{type}")]
        [HttpDelete]
        public HttpResponseMessage DeleteType(string type)
        {
            if (string.IsNullOrEmpty(type))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            using (var writer = TimeSeries.CreateWriter())
            {
                writer.DeleteType(type);
                writer.Commit();
            }
            
            TimeSeries.MetricsTimeSeries.ClientRequests.Mark();

            return new HttpResponseMessage(HttpStatusCode.NoContent);
        }

        [RavenRoute("ts/{timeSeriesName}/append/{type}")]
        [HttpPut]
        public async Task<HttpResponseMessage> AppendPoint()
        {
            var point = await ReadJsonObjectAsync<TimeSeriesFullPoint>().ConfigureAwait(false);
            if (point == null || string.IsNullOrEmpty(point.Type) || string.IsNullOrEmpty(point.Key) || point.Values == null || point.Values.Length == 0)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            using (var writer = TimeSeries.CreateWriter())
            {
                var newPointWasAppended = writer.Append(point.Type, point.Key, point.At, point.Values);
                writer.Commit();

                TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
                TimeSeries.Publisher.RaiseNotification(new TimeSeriesChangeNotification
                {
                    Type = point.Type,
                    Key = point.Key,
                    Action = TimeSeriesChangeAction.Append,
                    At = point.At,
                    Values = point.Values,
                });

                return GetEmptyMessage(newPointWasAppended ? HttpStatusCode.Created : HttpStatusCode.OK);
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

                var token = authorizer.GenerateSingleUseAuthToken(TimeSeriesName, User);
                return GetMessageWithObject(new
                {
                    Token = token
                });
            }

            if (HttpContext.Current != null)
                HttpContext.Current.Server.ScriptTimeout = 60 * 60 * 6; // six hours should do it, I think.

            var sp = Stopwatch.StartNew();
            var status = new BatchStatus();
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
                        using (var writer = TimeSeries.CreateWriter())
                        {
                            TimeSeries.Publisher.RaiseNotification(new BulkOperationNotification
                            {
                                Type = BatchType.Started,
                                OperationId = operationId
                            });

                            foreach (var change in changeBatch)
                            {
                                writer.Append(change.Type, change.Key, change.At, change.Values);
                            }
                            writer.Commit();

                            TimeSeries.Publisher.RaiseNotification(new BulkOperationNotification
                            {
                                Type = BatchType.Ended,
                                OperationId = operationId
                            });
                        }
                    }
                    status.MarkCompleted();
                }
                catch (OperationCanceledException)
                {
                    // happens on timeout
                    TimeSeries.Publisher.RaiseNotification(new BulkOperationNotification
                    {
                        Type = BatchType.Error,
                        OperationId = operationId,
                        Message = "Operation cancelled, likely because of a batch timeout"
                    });
                    
                    status.MarkCanceled("Operation cancelled, likely because of a batch timeout");
                    throw;
                }
                catch (Exception e)
                {
                    var errorMessage = e.SimplifyException().Message;
                    TimeSeries.Publisher.RaiseNotification(new BulkOperationNotification
                    {
                        Type = BatchType.Error,
                        OperationId = operationId,
                        Message = errorMessage
                    });

                    status.MarkFaulted(errorMessage);
                    throw;
                }
                finally
                {
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
                Description = operationId.ToString()
            }, out id, timeoutTokenSource);

            task.Wait(timeoutTokenSource.Token);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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

        private class BatchStatus : OperationStateBase
        {
            public int TimeSeries { get; set; }
        }

        [RavenRoute("ts/{timeSeriesName}/delete-key/{type}")]
        [HttpDelete]
        public HttpResponseMessage DeleteKey(string type, string key)
        {
            if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            using (var writer = TimeSeries.CreateWriter())
            {
                var pointsDeleted = writer.DeleteKey(type, key);
                writer.DeleteKeyInRollups(type, key);
                writer.Commit();

                TimeSeries.MetricsTimeSeries.Deletes.Mark();
                TimeSeries.Publisher.RaiseNotification(new TimeSeriesChangeNotification
                {
                    Type = type,
                    Key = key,
                    Action = TimeSeriesChangeAction.Delete,
                });

                return GetMessageWithObject(pointsDeleted);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/delete-points")]
        [HttpDelete]
        public async Task<HttpResponseMessage> DeletePoints()
        {
            var points = await ReadJsonObjectAsync<TimeSeriesPointId[]>().ConfigureAwait(false);
            if (points == null || points.Length == 0)
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            var deletedCount = 0;
            using (var writer = TimeSeries.CreateWriter())
            {
                foreach (var point in points)
                {
                    if (string.IsNullOrEmpty(point.Type))
                        throw new InvalidOperationException("Point type cannot be empty");
                    if (string.IsNullOrEmpty(point.Key))
                        throw new InvalidOperationException("Point key cannot be empty");

                    if (writer.DeletePoint(point))
                        deletedCount++;
                    writer.DeletePointInRollups(point);

                    TimeSeries.MetricsTimeSeries.Deletes.Mark();
                    TimeSeries.Publisher.RaiseNotification(new TimeSeriesChangeNotification
                    {
                        Type = point.Type,
                        Key = point.Key,
                        Action = TimeSeriesChangeAction.Delete,
                    });
                }
                writer.Commit();

                return GetMessageWithObject(deletedCount);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/delete-range/{type}")]
        [HttpDelete]
        public async Task<HttpResponseMessage> DeleteRange()
        {
            var range = await ReadJsonObjectAsync<TimeSeriesDeleteRange>().ConfigureAwait(false);
            if (range == null || string.IsNullOrEmpty(range.Type) || string.IsNullOrEmpty(range.Key))
                return GetEmptyMessage(HttpStatusCode.BadRequest);

            if (range.Start > range.End)
                throw new InvalidOperationException("start cannot be greater than end");

            using (var writer = TimeSeries.CreateWriter())
            {
                writer.DeleteRange(range.Type, range.Key, range.Start.UtcTicks, range.End.UtcTicks);
                writer.DeleteRangeInRollups(range.Type, range.Key, range.Start, range.End);
                writer.Commit();

                TimeSeries.MetricsTimeSeries.Deletes.Mark();
                TimeSeries.Publisher.RaiseNotification(new TimeSeriesChangeNotification
                {
                    Type = range.Type,
                    Key = range.Key,
                    Action = TimeSeriesChangeAction.DeleteInRange,
                    Start = range.Start,
                    End = range.End,
                });

                return new HttpResponseMessage(HttpStatusCode.NoContent);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/types")]
        [HttpGet]
        public HttpResponseMessage GetTypes(int skip = 0, int take = 20)
        {
            using (var reader = TimeSeries.CreateReader())
            {
                TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
                var types = reader.GetTypes(skip).Take(take).ToArray();
                return GetMessageWithObject(types);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/key/{type}")]
        [HttpGet]
        public HttpResponseMessage GetKey(string type, string key)
        {
            using (var reader = TimeSeries.CreateReader())
            {
                TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
                var result = reader.GetKey(type, key);
                return GetMessageWithObject(result);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/keys/{type}")]
        [HttpGet]
        public HttpResponseMessage GetKeys(string type, int skip = 0, int take = 20)
        {
            using (var reader = TimeSeries.CreateReader())
            {
                TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
                var keys = reader.GetKeys(type, skip).Take(take).ToArray();
                return GetMessageWithObject(keys);
            }
        }

        [RavenRoute("ts/{timeSeriesName}/points/{type}")]
        [HttpGet]
        public HttpResponseMessage GetPoints(string type, string key, int skip = 0, int take = 20, string start = null, string end = null)
        {
            if (skip < 0)
                return GetMessageWithString("Skip must be non-negative number", HttpStatusCode.BadRequest);
            if (take <= 0)
                return GetMessageWithString("Take must be non-negative number", HttpStatusCode.BadRequest);

            TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
            using (var reader = TimeSeries.CreateReader())
            {
                var points = reader.GetPoints(type, key, ParseDateTimeOffset(start) ?? DateTimeOffset.MinValue, ParseDateTimeOffset(end) ?? DateTimeOffset.MaxValue, skip).Take(take);
                return GetMessageWithObject(points);
            }
        }

        private DateTimeOffset? ParseDateTimeOffset(string date)
        {
            if (date == null)
                return null;

            DateTimeOffset result;
            if (DateTimeOffset.TryParseExact(date, new [] { "yyyy'-'MM'-'dd'T'HH':'mm':'ss.FFFFFFFK", "yyyy'-'MM'-'dd'T'HH':'mm':'ss.FFFFFFFzzz" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out result))
                return result;

            return null;
        }

        [RavenRoute("ts/{timeSeriesName}/aggregated-points/{type}")]
        [HttpGet]
        public HttpResponseMessage GetAggregatedPoints(string type, string key, AggregationDurationType durationType, int duration, int skip = 0, int take = 20, string start = null, string end = null)
        {
            if (skip < 0)
                return GetMessageWithString("Skip must be non-negative number", HttpStatusCode.BadRequest);
            if (take <= 0)
                return GetMessageWithString("Take must be non-negative number", HttpStatusCode.BadRequest);

            TimeSeries.MetricsTimeSeries.ClientRequests.Mark();
            using (var reader = TimeSeries.CreateReader())
            {
                var aggregatedPoints = reader.GetAggregatedPoints(type, key, new AggregationDuration(durationType, duration), ParseDateTimeOffset(start) ?? DateTimeOffset.MinValue, ParseDateTimeOffset(end) ?? DateTimeOffset.MaxValue, skip).Take(take);
                return GetMessageWithObject(aggregatedPoints);
            }
        }
    }
}