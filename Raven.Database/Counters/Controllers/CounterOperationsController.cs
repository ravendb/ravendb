using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
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
using BatchType = Raven.Abstractions.Counters.Notifications.BatchType;

namespace Raven.Database.Counters.Controllers
{
    public class CounterOperationsController : BaseCountersApiController
    {
        [RavenRoute("cs/{counterStorageName}/sinceEtag")]
        [HttpGet]
        public HttpResponseMessage GetCounterStatesSinceEtag(long etag, int skip, int take)
        {
            List<CounterState> deltas;
            using (var reader = CounterStorage.CreateReader())
                deltas = reader.GetCountersSinceEtag(etag + 1, skip, take).ToList();

            return GetMessageWithObject(deltas);
        }

        [RavenRoute("cs/{counterStorageName}/change")]
        [HttpPost]
        public HttpResponseMessage Change(string groupName, string counterName, long delta)
        {
            var verificationResult = VerifyGroupAndCounterName(groupName, counterName);
            if (verificationResult != null)
                return verificationResult;

            CounterChangeAction counterChangeAction;
            long? total = 0;
            using (var writer = CounterStorage.CreateWriter())
            {
                counterChangeAction = writer.Store(groupName, counterName, delta);
                if (delta == 0 && counterChangeAction != CounterChangeAction.Add)
                    return GetEmptyMessage();

                writer.Commit();
                if (!writer.TryGetCounterTotal(groupName, counterName,out total))
                {
                    return GetMessageWithObject(new
                    {
                        Message = $"Could not find a counter with groupName = {groupName}, counterName = {counterName}"
                    },HttpStatusCode.NotFound);
                }
            }

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            CounterStorage.Publisher.RaiseNotification(new ChangeNotification
            {
                GroupName = groupName,
                CounterName = counterName,
                Action = counterChangeAction,
                Delta = delta,
                Total = total.Value
            });

            return GetEmptyMessage();
        }

        [RavenRoute("cs/{counterStorageName}/groups")]
        [HttpGet]
        public HttpResponseMessage GetCounterGroups(int skip, int take)
        {
            using (var reader = CounterStorage.CreateReader())
            {
                CounterStorage.MetricsCounters.ClientRequests.Mark();
                return Request.CreateResponse(HttpStatusCode.OK, reader.GetCounterGroups(skip, take).ToList());
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

                var token = authorizer.GenerateSingleUseAuthToken(CountersName, User);
                return GetMessageWithObject(new
                {
                    Token = token
                });
            }

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            if (HttpContext.Current != null)
                HttpContext.Current.Server.ScriptTimeout = 60 * 60 * 6; // six hours should do it, I think.

            var sp = Stopwatch.StartNew();
            var status = new BatchStatus();
            var timeoutTokenSource = new CancellationTokenSource();
            var counterChanges = 0;

            var operationId = ExtractOperationId();
            var inputStream = await InnerRequest.Content.ReadAsStreamAsync().ConfigureAwait(false);
            var task = Task.Factory.StartNew(() =>
            {
                var timeout = timeoutTokenSource.TimeoutAfter(SystemConfiguration.Counter.BatchTimeout); 
                var changeBatches = YieldChangeBatches(inputStream, timeout, countOfChanges => counterChanges += countOfChanges);
                try
                {
                    foreach (var changeBatch in changeBatches)
                    {
                        using (var writer = CounterStorage.CreateWriter())
                        {
                            CounterStorage.Publisher.RaiseNotification(new BulkOperationNotification
                            {
                                Type = BatchType.Started,
                                OperationId = operationId
                            });

                            foreach (var change in changeBatch)
                            {
                                var verificationResult = VerifyGroupAndCounterName(change.Group, change.Name);
                                if (verificationResult != null)
                                    throw new ArgumentException("Missing group or counter name");
                                writer.Store(change.Group, change.Name, change.Delta);
                            }
                            writer.Commit();

                            CounterStorage.Publisher.RaiseNotification(new BulkOperationNotification
                            {
                                Type = BatchType.Ended,
                                OperationId = operationId
                            });
                        }
                    }
                    status.MarkCompleted(string.Format("Counters: {0}", counterChanges));
                }
                catch (OperationCanceledException)
                {
                    // happens on timeout
                    CounterStorage.Publisher.RaiseNotification(new BulkOperationNotification
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
                    CounterStorage.Publisher.RaiseNotification(new BulkOperationNotification
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
                    status.Counters = counterChanges;
                }
            }, timeoutTokenSource.Token);


            long id;
            DatabasesLandlord.SystemDatabase.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.CounterBatchOperation,
                Description = operationId.ToString()
            }, out id, timeoutTokenSource);

            AddRequestTraceInfo(log => 
                log.AppendFormat("\tCounters batch operation received {0:#,#;;0} changes in {1}, long running task Id : {2}", counterChanges, sp.Elapsed, id));

            task.Wait(timeoutTokenSource.Token);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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

        private class BatchStatus : OperationStateBase
        {
            public int Counters { get; set; }
        }

        [RavenRoute("cs/{counterStorageName}/reset")]
        [HttpPost]
        public HttpResponseMessage Reset(string groupName, string counterName)
        {
            var verificationResult = VerifyGroupAndCounterName(groupName, counterName);
            if (verificationResult != null)
                return verificationResult;

            long difference = 0;
            using (var writer = CounterStorage.CreateWriter())
            {
                difference = writer.Reset(groupName, counterName);
                if (difference != 0)
                {
                    writer.Commit();
                }
            }

            if (difference != 0)
            {
                CounterStorage.MetricsCounters.ClientRequests.Mark();
                CounterStorage.MetricsCounters.Resets.Mark();
                CounterStorage.Publisher.RaiseNotification(new ChangeNotification
                {
                    GroupName = groupName,
                    CounterName = counterName,
                    Action = difference >= 0 ? CounterChangeAction.Increment : CounterChangeAction.Decrement,
                    Delta = difference,
                    Total = 0
                });
            }

            return GetEmptyMessage();
        }

        [RavenRoute("cs/{counterStorageName}/delete")]
        [HttpDelete]
        public HttpResponseMessage Delete(string groupName, string counterName)
        {
            var verificationResult = VerifyGroupAndCounterName(groupName, counterName);
            if (verificationResult != null)
                return verificationResult;

            using (var writer = CounterStorage.CreateWriter())
            {
                writer.Delete(groupName, counterName);
                writer.Commit();
            }

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            CounterStorage.MetricsCounters.Deletes.Mark();
            CounterStorage.Publisher.RaiseNotification(new ChangeNotification
            {
                GroupName = groupName,
                CounterName = counterName,
                Action = CounterChangeAction.Delete,
                Delta = 0,
                Total = 0
            });

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [RavenRoute("cs/{counterStorageName}/delete-by-group")]
        [HttpDelete]
        public HttpResponseMessage DeleteByGroup(string groupName)
        {
            groupName = groupName ?? string.Empty;
            var deletedCount = 0;

            while (true)
            {
                var changeNotifications = new List<ChangeNotification>();
                using (var writer = CounterStorage.CreateWriter())
                {

                    var countersDetails = writer.GetCountersDetails(groupName).Take(1024).ToList();
                    if (countersDetails.Count == 0)
                        break;

                    foreach (var c in countersDetails)
                    {
                        writer.DeleteCounterInternal(c.Group, c.Name);
                        changeNotifications.Add(new ChangeNotification
                        {
                            GroupName = c.Group,
                            CounterName = c.Name,
                            Action = CounterChangeAction.Delete,
                            Delta = 0,
                            Total = 0
                        });
                    }
                    writer.Commit();

                    deletedCount += changeNotifications.Count;
                }

                CounterStorage.MetricsCounters.ClientRequests.Mark();
                changeNotifications.ForEach(change =>
                {
                    CounterStorage.Publisher.RaiseNotification(change);
                    CounterStorage.MetricsCounters.Deletes.Mark();
                });
            }

            return GetMessageWithObject(deletedCount);
        }	

        [RavenRoute("cs/{counterStorageName}/counters")]
        [HttpGet]
        public HttpResponseMessage GetCounterSummariesByGroup(int skip, int take, string group)
        {
            if (skip < 0)
                return GetMessageWithString("Skip must be non-negative number", HttpStatusCode.BadRequest);
            if (take <= 0)
                return GetMessageWithString("Take must be non-negative number", HttpStatusCode.BadRequest);

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            using (var reader = CounterStorage.CreateReader())
            {
                group = group ?? string.Empty;
                var counters = reader.GetCounterSummariesByGroup(group, skip, take);
                return GetMessageWithObject(counters);
            }
        }

        [RavenRoute("cs/{counterStorageName}/getCounterOverallTotal")]
        [HttpGet]
        public HttpResponseMessage GetCounterOverallTotal(string groupName, string counterName)
        {
            var verificationResult = VerifyGroupAndCounterName(groupName, counterName);
            if (verificationResult != null)
                return verificationResult;

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            using (var reader = CounterStorage.CreateReader())
            {
                try
                {
                    long? total;
                    if (!reader.TryGetCounterTotal(groupName, counterName, out total))
                        return GetMessageWithObject(new CounterTotal {IsExists = false},HttpStatusCode.NotFound);

                    Debug.Assert(total.HasValue);
                    return GetMessageWithObject(new CounterTotal { IsExists = true, Total = total.Value});
                }
                catch (Exception e)
                {
                    return Request.CreateErrorResponse(HttpStatusCode.BadRequest, e);
                }
            }
        }

        [RavenRoute("cs/{counterStorageName}/by-prefix")]
        [HttpGet]
        public HttpResponseMessage GetCountersByPrefix(string groupName, int skip, int take, string counterNamePrefix = null)
        {
            if (string.IsNullOrWhiteSpace(groupName))
                return GetMessageWithObject(new
                {
                    Message = $"{nameof(groupName)} is a required parameter, thus it must not be null or empty"
                },HttpStatusCode.BadRequest);

            HttpResponseMessage messageWithObject;
            if (!ValidateSkipAndTake(skip, take, out messageWithObject))
                return messageWithObject;

            using (var reader = CounterStorage.CreateReader())
            {
                var counterSummaries = reader.GetCounterSummariesByPrefix(groupName, counterNamePrefix, skip, take).ToList();
                return GetMessageWithObject(counterSummaries);
            }
        }

        private bool ValidateSkipAndTake(int skip, int take, out HttpResponseMessage messageWithObject)
        {
            messageWithObject = null;

            if (skip < 0)
            {
                messageWithObject = GetMessageWithObject(new
                {
                    Message = $"{nameof(skip)} must not be negative number."
                }, HttpStatusCode.BadRequest);
                return false;
            }

            if (take <= 0)
            {
                messageWithObject = GetMessageWithObject(new
                {
                    Message = $"{nameof(take)} is a required parameter and must not be less or equal to zero"
                }, HttpStatusCode.BadRequest);
                return false;
            }
            return true;
        }

        [RavenRoute("cs/{counterStorageName}/getCounter")]
        [HttpGet]
        public HttpResponseMessage GetCounter(string groupName, string counterName)
        {
            var verificationResult = VerifyGroupAndCounterName(groupName, counterName);
            if (verificationResult != null)
                return verificationResult;

            CounterStorage.MetricsCounters.ClientRequests.Mark();
            using (var reader = CounterStorage.CreateReader())
            {
                var result = reader.GetCounter(groupName, counterName);
                return GetMessageWithObject(result);
            }
        }

        [RavenRoute("cs/{counterStorageName}/purge-tombstones")]
        [HttpPost]
        public HttpResponseMessage PurgeTombstones()
        {
            CounterStorage.MetricsCounters.ClientRequests.Mark();

            while (true)
            {
                using (var writer = CounterStorage.CreateWriter())
                {
                    if (writer.PurgeOutdatedTombstones() == false)
                        break;

                    writer.Commit();
                }
            }

            return GetEmptyMessage();
        }

        // ReSharper disable once UnusedParameter.Local
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private HttpResponseMessage VerifyGroupAndCounterName(string groupName, string counterName)
        {
            if (string.IsNullOrEmpty(groupName))
            {
                return GetMessageWithString("Group name is mandatory.", HttpStatusCode.BadRequest);
            }

            if (string.IsNullOrEmpty(counterName))
            {
                return GetMessageWithString("Counter name is mandatory.", HttpStatusCode.BadRequest);
            }

            return null;
        }
    }
}
