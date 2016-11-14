using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Counters;
using Raven.NewClient.Abstractions.Exceptions;
using Raven.NewClient.Abstractions.Json;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Extensions;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.NewClient.Json.Linq;
using Sparrow;

namespace Raven.NewClient.Client.Counters.Operations
{
    public sealed class CountersBatchOperation : CounterOperationsBase, IDisposable
    {
        private readonly CountersBatchOptions defaultOptions;
        private readonly AsyncManualResetEvent streamingStarted;
        private readonly TaskCompletionSource<bool> batchOperationTcs;
        private readonly CancellationTokenSource cts;
        private readonly BlockingCollection<CounterChange> changesQueue;
        private readonly string singleAuthUrl;

        private bool disposed;
        private Task batchOperationTask;
        private Timer closeAndReopenStreamingTimer;
        private MemoryStream tempStream;
        private long serverOperationId;

        public Guid OperationId { get; private set; }

        public CountersBatchOptions DefaultOptions { get { return defaultOptions; } }		

        internal CountersBatchOperation(CounterStore parent, string counterStorageName, CountersBatchOptions batchOptions = null)
            : base(parent, counterStorageName)
        {
            if(batchOptions != null && batchOptions.BatchSizeLimit < 1)
                throw new ArgumentException("batchOptions.BatchSizeLimit cannot be negative", "batchOptions");

            defaultOptions = batchOptions ?? new CountersBatchOptions(); //defaults do exist
            streamingStarted = new AsyncManualResetEvent();
            batchOperationTcs = new TaskCompletionSource<bool>();
            cts = new CancellationTokenSource();
            changesQueue = new BlockingCollection<CounterChange>(defaultOptions.BatchSizeLimit);			
            singleAuthUrl = string.Format("{0}/cs/{1}/singleAuthToken", ServerUrl, counterStorageName);

            OperationId = Guid.NewGuid();
            disposed = false;
            batchOperationTask = StartBatchOperation();
            if (AsyncHelpers.RunSync(() => streamingStarted.WaitAsync(TimeSpan.FromMilliseconds(DefaultOptions.StreamingInitializeTimeout))) == false ||
                batchOperationTask.IsFaulted)
            {
                throw new InvalidOperationException("Failed to start streaming batch.", batchOperationTask.Exception);
            }
            closeAndReopenStreamingTimer = CreateNewTimer();
        }

        private async Task StartBatchOperation()
        {
            if (tempStream != null)
                tempStream.Dispose();

            tempStream = new MemoryStream();
            streamingStarted.Reset();
            using (ConnectionOptions.Expect100Continue(ServerUrl))
            {
                var authToken = await GetToken().ConfigureAwait(false);
                try
                {
                    authToken = await ValidateThatWeCanUseAuthenticateTokens(authToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Could not authenticate token for bulk insert, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration", e);
                }

                var requestUriString = string.Format("{0}/batch?operationId={1}", CounterStorageUrl, OperationId);
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post, true, true)
                                        .AddOperationHeader("Single-Use-Auth-Token", authToken))
                {
                    var token = cts.Token;

                    var response = await request.ExecuteRawRequestAsync((stream, source) => Task.Factory.StartNew(() =>
                    {
                        streamingStarted.Set();
                        try
                        {
                            ContinuouslyWriteQueueToServer(stream, token);
                            source.TrySetResult(null);
                        }
                        catch (Exception e)
                        {
                            source.TrySetException(e);
                            batchOperationTcs.SetException(e);
                        }
                    }, TaskCreationOptions.LongRunning)).ConfigureAwait(false);

                    await response.AssertNotFailingResponse().ConfigureAwait(false);

                    using (response)
                    {
                        using (var stream = await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false))
                        {
                            var result = RavenJToken.TryLoad(stream); 
                            if (result == null) //precaution - should prevent NRE in case the crap hits the fan
                                throw new InvalidOperationException("Invalid response from server... maybe its not json?");

                            serverOperationId = result.Value<long>("OperationId");
                        }
                    }

                    await IsOperationCompleted(serverOperationId).ConfigureAwait(false);
                    batchOperationTcs.SetResult(true);
                }
            }
        }

        private void CloseAndReopenStreaming(object state)
        {
            cts.Cancel();
            if (batchOperationTask.Status != TaskStatus.Faulted &&
                batchOperationTask.Status != TaskStatus.Canceled)
                AsyncHelpers.RunSync(() => batchOperationTask);

            batchOperationTask = StartBatchOperation();

            closeAndReopenStreamingTimer.Dispose();
            closeAndReopenStreamingTimer = CreateNewTimer();
        }

        private Timer CreateNewTimer()
        {
            return new Timer(CloseAndReopenStreaming, null,
                TimeSpan.FromMilliseconds(defaultOptions.ConnectionReopenTimingInMilliseconds),
                TimeSpan.FromMilliseconds(-1)); //fire timer only once -> then rescedule - handles ConnectionReopenTimingInMilliseconds changing use-case
        }

        private async Task<bool> IsOperationCompleted(long operationId)
        {
            ErrorResponseException errorResponse;

            try
            {
                var status = await GetOperationStatusAsync(operationId).ConfigureAwait(false);

                if (status == null)
                    return true;

                if (status.Value<bool>("Completed"))
                    return true;

                return false;
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.Conflict)
                    throw;

                errorResponse = e;
            }

            var errorMessage = RavenJObject.Load(new RavenJsonTextReader(new StringReader(errorResponse.ResponseString)));

            throw new ConcurrencyException(errorMessage.Value<string>("Error"));
        }

        private async Task<RavenJToken> GetOperationStatusAsync(long id)
        {
            var url = ServerUrl + "/operation/status?id=" + id;
            using (var request = CreateHttpJsonRequest(url, HttpMethods.Get))
            {
                try
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound) return null;
                    throw;
                }
            }
        }

        private void ContinuouslyWriteQueueToServer(Stream stream, CancellationToken token)
        {
            try
            {
                var batch = new List<CounterChange>();
                while (changesQueue.IsCompleted == false)
                {
                    batch.Clear();
                    if (token.IsCancellationRequested)
                    {
                        FetchAllChangeQueue(batch);	
                        FlushToServer(stream,batch);
                        break;
                    }

                    TaskCompletionSource<object> tcs = null;
                    try
                    {
                        CounterChange counterChange;
                        while (changesQueue.TryTake(out counterChange, DefaultOptions.BatchReadTimeoutInMilliseconds, token))
                        {
                            if (counterChange.Done != null)
                            {
                                tcs = counterChange.Done;
                                break;
                            }

                            batch.Add(counterChange);
                            if (batch.Count >= DefaultOptions.BatchSizeLimit)
                                break;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        if(changesQueue.Count > 0)
                            FetchAllChangeQueue(batch);
                    }

                    try
                    {
                        FlushToServer(stream, batch);
                        if (tcs != null)
                            tcs.TrySetResult(null);
                    }
                    catch (OperationCanceledException)
                    {
                        if (tcs != null)
                            tcs.TrySetResult(null);						
                    }
                    catch (Exception e)
                    {
                        if (tcs != null)
                            tcs.TrySetException(e);
                    }
                    
                }
            }
            finally
            {
                tempStream.Dispose();
            }
        }

        private void FetchAllChangeQueue(List<CounterChange> batch)
        {
            CounterChange counterChange;
            while (changesQueue.TryTake(out counterChange))
                batch.Add(counterChange);
        }

        public Task FlushAsync()
        {
            var taskCompletionSource = new TaskCompletionSource<object>();
            changesQueue.Add(new CounterChange
            {
                Done = taskCompletionSource
            });

            return taskCompletionSource.Task.ContinueWith(t => CloseAndReopenStreaming(null));
        }

        private void FlushToServer(Stream requestStream, ICollection<CounterChange> batchItems)
        {
            if (batchItems.Count == 0)
                return;

            tempStream.SetLength(0);
            long bytesWritten;
            WriteCollectionToBuffer(tempStream, AggregateItems(batchItems), out bytesWritten);

            var requestBinaryWriter = new BinaryWriter(requestStream);
            requestBinaryWriter.Write((int)tempStream.Position);

            tempStream.WriteTo(requestStream);
            requestStream.Flush();
        }

        private static ICollection<CounterChange> AggregateItems(IEnumerable<CounterChange> batchItems)
        {
            var aggregationResult = from item in batchItems
                                    group item by new { item.Name, item.Group }
                                        into g
                                        select new CounterChange
                                        {
                                            Name = g.Key.Name,
                                            Group = g.Key.Group,
                                            Delta = g.Sum(x => x.Delta)
                                        };

            var aggregateItems = aggregationResult.ToList();
            return aggregateItems;
        }

        private static void WriteCollectionToBuffer(Stream targetStream, ICollection<CounterChange> items, out long bytesWritten)
        {
            using (var gzip = new GZipStream(targetStream, CompressionMode.Compress, leaveOpen: true))
            using (var stream = new CountingStream(gzip))
            {
                var binaryWriter = new BinaryWriter(stream);
                binaryWriter.Write(items.Count);
                var bsonWriter = new BsonWriter(binaryWriter)
                {
                    DateTimeKindHandling = DateTimeKind.Unspecified
                };

                foreach (var doc in items.Select(RavenJObject.FromObject))
                    doc.WriteTo(bsonWriter);

                bsonWriter.Flush();
                binaryWriter.Flush();
                stream.Flush();
                bytesWritten = stream.NumberOfWrittenBytes;
            }
        }

        public void ScheduleChange(string groupName, string counterName, long delta)
        {
            changesQueue.Add(new CounterChange
            {
                Delta = delta,
                Group = groupName,
                Name = counterName
            });
        }

        public void ScheduleIncrement(string groupName, string counterName)
        {
            ScheduleChange(groupName, counterName, 1);
        }

        public void ScheduleDecrement(string groupName, string counterName)
        {
            ScheduleChange(groupName, counterName, -1);
        }

        public void Dispose()
        {
            if (disposed)
                return;

            closeAndReopenStreamingTimer.Dispose();
            changesQueue.CompleteAdding();

            batchOperationTcs.Task.Wait();
            if (batchOperationTask.Status != TaskStatus.RanToCompletion ||
                batchOperationTask.Status != TaskStatus.Canceled)
                cts.Cancel();

            if (serverOperationId != default(long))
            {
                while (true)
                {
                    var serverSideOperationWaitingTask = IsOperationCompleted(serverOperationId);
                    serverSideOperationWaitingTask.Wait();

                    if (serverSideOperationWaitingTask.Result)
                        break;

                    Thread.Sleep(100);
                }
            }

            tempStream.Dispose(); //precaution
            disposed = true;
        }

        private async Task<string> GetToken()
        {
            // this will force the HTTP layer to authenticate, meaning that our next request won't have to
            var jsonToken = await GetAuthToken().ConfigureAwait(false);

            return jsonToken.Value<string>("Token");
        }

        private async Task<RavenJToken> GetAuthToken()
        {
            using (var request = CreateHttpJsonRequest(singleAuthUrl, HttpMethods.Get, disableRequestCompression: true))
            {
                var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                return response;
            }
        }

        private async Task<string> ValidateThatWeCanUseAuthenticateTokens(string token)
        {
            using (var request = CreateHttpJsonRequest(singleAuthUrl, HttpMethods.Get, disableRequestCompression: true, disableAuthentication: true))
            {
                request.AddOperationHeader("Single-Use-Auth-Token", token);
                var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                return result.Value<string>("Token");
            }
        }
    }
}
