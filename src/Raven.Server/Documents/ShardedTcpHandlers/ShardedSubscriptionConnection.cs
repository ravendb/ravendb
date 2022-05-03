// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase
    {
        private readonly Dictionary<string, SubscriptionShardHolder> _shardWorkers = new Dictionary<string, SubscriptionShardHolder>();
        public static ConcurrentDictionary<string, ShardedSubscriptionConnection> Connections = new ConcurrentDictionary<string, ShardedSubscriptionConnection>();
        public  AsyncManualResetEvent _mre;

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.DatabaseContext.DatabaseName, tcpConnection.DatabaseContext.DatabaseShutdown)
        {
            _mre = new AsyncManualResetEvent();
        }

       /*
        * The subscription worker (client) connects to an orchestrator (SendShardedSubscriptionDocuments). 
        * The orchestrator initializes shard subscription workers (StartShardSubscriptionWorkersAsync).
        * ShardedSubscriptionWorkers are maintaining the connection with subscription on each shard.
        * The orchestrator maintains the connection with the client and checks if there is an available batch on each Sharded Worker (MaintainConnectionWithClientWorkerAsync).
        * Handle batch flow:
        * Orchestrator sends the batch to the client (WriteBatchToClientAndAckAsync).
        * Orchestrator receive batch ACK request from client.
        * Orchestrator advances the sharded worker and waits for the sharded worker.
        * Sharded worker sends an ACK to the shard and waits for CONFIRM from shard (ACK command in cluster)
        * Sharded worker advances the Orchestrator
        * Orchestrator sends the CONFIRM to client
        */
        public static void SendShardedSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress($"ShardedSubscription_{tcpConnectionOptions.DatabaseContext.DatabaseName}");
            try
            {
                var connection = new ShardedSubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                _ = connection.RunAsync();
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();
                throw;
            }
        }

        public async Task RunAsync()
        {
            try
            {
                await ParseSubscriptionOptionsAsync();
                await StartShardSubscriptionWorkersAsync();
                await MaintainConnectionWithClientWorkerAsync();
            }
            catch (Exception e)
            {
                await ReportExceptionAsync(SubscriptionError.Error, ConnectionException ?? e);
            }
            finally
            {
                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing sharded subscription."));
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Finished processing sharded subscription '{SubscriptionId}' / from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");

                Connections.TryRemove(_options.SubscriptionName, out _);
                Dispose();
            }
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for sharded database '{Database}' on '{_serverStore.NodeTag}'",
                ClientType = $"'client worker' with IP '{TcpConnection.TcpClient.Client.RemoteEndPoint}'",
                SubscriptionType = $"sharded subscription '{_options?.SubscriptionName}', id '{SubscriptionId}'"
            };
        }

        public override async Task ParseSubscriptionOptionsAsync()
        {
            await base.ParseSubscriptionOptionsAsync();

            // we want to limit the batch of each shard, to not hold too much memory if there are other batches while batch is proceed
            _options.MaxDocsPerBatch = Math.Min(_options.MaxDocsPerBatch / TcpConnection.DatabaseContext.ShardCount, _options.MaxDocsPerBatch);

            // add to connections
            if (Connections.TryAdd(_options.SubscriptionName, this) == false)
            {
                throw new InvalidOperationException($"Sharded subscription '{_options.SubscriptionName}' on '{Database}' already exists!");
            } 
        }

        protected override async Task ReportExceptionAsync(SubscriptionError error, Exception e)
        {
            try
            {
                await LogExceptionAndReportToClientAsync(ConnectionException ?? e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        protected override Task OnClientAckAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task SendNoopAckAsync()
        {
            return Task.CompletedTask;
        }

        internal override Task<SubscriptionConnectionClientMessage> GetReplyFromClientAsync()
        {
            return null;
        }

        private async Task StartShardSubscriptionWorkersAsync()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Create));

            for (int i = 0; i < TcpConnection.DatabaseContext.ShardCount; i++)
            {
                var re = TcpConnection.DatabaseContext.ShardExecutor.GetRequestExecutorAt(i);
                var shard = ShardHelper.ToShardName(TcpConnection.DatabaseContext.DatabaseName, i);
                var worker = CreateShardedWorkerHolder(shard, re, lastErrorDateTime: null);

                _shardWorkers.Add(shard, worker);
            }

            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            });
        }

        private SubscriptionShardHolder CreateShardedWorkerHolder(string shard, RequestExecutor re, DateTime? lastErrorDateTime)
        {
            var shardWorker = new ShardedSubscriptionWorker(_options, shard, re, parent: this);

            var holder = new SubscriptionShardHolder(shardWorker, shardWorker.RunInternalAsync(CancellationTokenSource.Token), re)
            {
                LastErrorDateTime = lastErrorDateTime
            };

            return holder;
        }

        private async Task MaintainConnectionWithClientWorkerAsync()
        {
            var hasBatch = _mre.WaitAsync(CancellationTokenSource.Token);
            while (CancellationTokenSource.IsCancellationRequested == false)
            {
                var whenAny = await Task.WhenAny(TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs), CancellationTokenSource.Token), hasBatch);
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                HandleBatchFromWorkersResult result;
                if (whenAny == hasBatch)
                {
                    Interlocked.Exchange(ref _mre, new AsyncManualResetEvent());
                    hasBatch = _mre.WaitAsync(CancellationTokenSource.Token);
                    await HandleBatchFromWorkersAsync();
                }
                else
                {
                    await SendHeartBeatAsync("Waited for 3000ms for batch from shard workers");
                }

                result = await CheckWorkersForErrorsAsync();

                if (result.Stopping)
                    ThrowStoppingSubscriptionException(result);

                ReconnectWorkersIfNeeded(result.ShardsToReconnect);
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        private void ThrowStoppingSubscriptionException(HandleBatchFromWorkersResult result)
        {
            throw new ShardedSubscriptionException(
                $"Stopping sharded subscription '{_options.SubscriptionName}' with id '{SubscriptionId}' " +
                $"for database '{TcpConnection.DatabaseContext.DatabaseName}' because " +
                $"shard {string.Join(", ", result.Exceptions.Keys)} workers failed. " +
                $"Additional Reason: {result.StoppingReason ?? string.Empty}",
                result.Exceptions.Values);
        }

        private void ReconnectWorkersIfNeeded(List<string> shardsToReconnect)
        {
            if (shardsToReconnect.Count == 0)
                return;

            foreach (var shard in shardsToReconnect)
            {
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (_shardWorkers.ContainsKey(shard) == false)
                    continue;

                using (var old = _shardWorkers[shard])
                {
                    var holder = CreateShardedWorkerHolder(shard, old.RequestExecutor, old.LastErrorDateTime);
                    _shardWorkers[shard] = holder;
                }
            }
        }

        private async Task HandleBatchFromWorkersAsync()
        {
            var batchesFromShards = new List<ShardedSubscriptionWorker.PublishedShardBatch>();

            foreach ((string shard, SubscriptionShardHolder shardHolder) in _shardWorkers)
            {
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (shardHolder.PullingTask.IsCompleted == false)
                {
                    var batch = shardHolder.Worker.PublishedShardBatchItem;
                    if (batch == null)
                        continue;

                    batchesFromShards.Add(batch);
                }
            }

            if (batchesFromShards.Count > 0)
            {
                await WriteBatchesAndConfirmAsync(batchesFromShards);
            }
        }

        private async Task WriteBatchToClientAndAckAsync(List<ShardedSubscriptionWorker.PublishedShardBatch> batches)
        {
            var replyFromClientTask = GetReplyFromClientInternalAsync();
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to send documents."));
            int docsToFlush = 0;

            var shards = string.Join(", ", batches.Select(b => b.ShardNumber));
            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.CheckoutMemoryStream(out var buffer))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, buffer))
            {
                foreach (ShardedSubscriptionWorker.PublishedShardBatch batch in batches)
                {
                    string lastReceivedChangeVector = null;
                    foreach (var doc in batch._batchFromServer.Messages)
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();
                        (BlittableJsonReaderObject metadata, string id, string changeVector) = BatchFromServer.GetMetadataFromBlittable(doc.Data);
                        lastReceivedChangeVector = changeVector;
                        SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, doc.Data, metadata, doc.Exception, id, null, null, null);
                        docsToFlush++;

                        if (await SubscriptionConnection.FlushBatchIfNeededAsync(sendingCurrentBatchStopwatch, SubscriptionId, writer, buffer, TcpConnection, stats: null, _logger, docsToFlush, CancellationTokenSource.Token))
                            continue;

                        docsToFlush = 0;
                        sendingCurrentBatchStopwatch.Restart();
                    }
                    batch.LastSentChangeVectorInBatch = lastReceivedChangeVector;
                    AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Flushing docs collected from shard '{batch.ShardNumber}'"));
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major, "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
                SubscriptionConnection.WriteEndOfBatch(writer);

                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Flushing docs collected from '{shards}'"));
                await SubscriptionConnection.FlushDocsToClientAsync(SubscriptionId, writer, buffer, TcpConnection, stats: null, _logger, docsToFlush, endOfBatch: true, CancellationTokenSource.Token);
            }

            await WaitForClientAck(replyFromClientTask);
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Got ack for shards '{shards}' from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
        }

        private async Task WriteBatchesAndConfirmAsync(List<ShardedSubscriptionWorker.PublishedShardBatch> batches)
        {
            try
            {
                // Send to client
                await WriteBatchToClientAndAckAsync(batches);

                // let sharded subscription worker know that we sent the batch to the client and received an ack request from it
                foreach (var batch in batches)
                {
                    batch.SendBatchToClientTcs.SetResult();
                }
            }
            catch (Exception e)
            {
                // need to fail the shard subscription worker
                foreach (var batch in batches)
                {
                    batch.SendBatchToClientTcs.SetException(e);
                }
                throw;
            }

            // wait for sharded subscription worker to send ack to the shard subscription connection
            // and receive the confirm from the shard subscription connection
            foreach (var batch in batches)
            {
                await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;
            }

            // send confirm to client and continue processing
            await SendConfirmToClientAsync();
        }

        private async Task<HandleBatchFromWorkersResult> CheckWorkersForErrorsAsync()
        {
            var result = new HandleBatchFromWorkersResult
            {
                Exceptions = new Dictionary<string, Exception>(), 
                ShardsToReconnect = new List<string>(), 
                Stopping = false
            };

            foreach ((string shard, SubscriptionShardHolder shardHolder) in _shardWorkers)
            {
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (result.Stopping)
                    continue;

                if (shardHolder.PullingTask.IsCompleted)
                {
                    try
                    {
                        await shardHolder.PullingTask;
                        Debug.Assert(false, $"The pulling task should be faulted or canceled. Should not reach this line");
                    }
                    catch (Exception e)
                    {
                        result.Exceptions.Add(shard, e);
                        result.ShardsToReconnect.Add(shard);
                    }
                }

                if (CanContinueSubscription(shardHolder))
                    continue;

                // we are stopping this subscription
                result.Stopping = true;
                result.StoppingReason = $"Hit {nameof(SubscriptionWorkerOptions.MaxErroneousPeriod)}.";
            }

            if (result.Exceptions.Count == _shardWorkers.Count && result.Stopping == false)
            {
                // stop subscription if all workers have unrecoverable exception
                result.Stopping = CanStopSubscription(result.Exceptions);
            }

            return result;
        }

        private bool CanStopSubscription(IReadOnlyDictionary<string, Exception> exceptions)
        {
            bool stopping = true;
            foreach (var worker in _shardWorkers)
            {
                var ex = exceptions[worker.Key];
                Debug.Assert(ex != null, "ex != null");

                (bool shouldTryToReconnect, _) = worker.Value.Worker.CheckIfShouldReconnectWorker(ex, CancellationTokenSource, assertLastConnectionFailure: null, onUnexpectedSubscriptionError: null, throwOnRedirectNodeNotFound: false);
                if (shouldTryToReconnect)
                {
                    // we have at least one worker to try to reconnect
                    stopping = false;
                }
            }

            return stopping;
        }

        private bool CanContinueSubscription(SubscriptionShardHolder shardHolder)
        {
            if (shardHolder.LastErrorDateTime.HasValue == false)
            {
                shardHolder.LastErrorDateTime = DateTime.UtcNow;
                return true;
            }

            if (DateTime.UtcNow - shardHolder.LastErrorDateTime.Value <= _options.MaxErroneousPeriod)
                return true;

            return false;
        }

        internal async Task SendConfirmToClientAsync()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Shard subscription connection '{SubscriptionId}' send confirm to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm)
            });
        }

        public override void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;

            base.Dispose();

            foreach (var worker in _shardWorkers)
            {
                worker.Value.Dispose();
            }
        }

        private class SubscriptionShardHolder : IDisposable
        {
            public readonly ShardedSubscriptionWorker Worker;
            public readonly Task PullingTask;
            public readonly RequestExecutor RequestExecutor;
            public DateTime? LastErrorDateTime;

            public SubscriptionShardHolder(ShardedSubscriptionWorker worker, Task pullingTask, RequestExecutor requestExecutor)
            {
                Worker = worker;
                PullingTask = pullingTask;
                RequestExecutor = requestExecutor;
            }

            public void Dispose()
            {
                Worker.Dispose();
            }
        }

        private class HandleBatchFromWorkersResult
        {
            public Dictionary<string, Exception> Exceptions;
            public List<string> ShardsToReconnect;
            public bool Stopping;
            public string StoppingReason;
        }
    }
}
