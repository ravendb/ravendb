// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public class OrchestratedSubscriptionConnection : SubscriptionConnectionBase
    {
        private readonly Dictionary<string, SubscriptionShardHolder> _shardWorkers = new Dictionary<string, SubscriptionShardHolder>();
        private SubscriptionConnectionsStateOrchestrator _state;

        public OrchestratedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable,
            JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.DatabaseContext.DatabaseName,
                tcpConnection.DatabaseContext.DatabaseShutdown)
        {
        }

        public SubscriptionConnectionsStateOrchestrator GetOrchestratedSubscriptionConnectionState()
        {
            var subscriptions = TcpConnection.DatabaseContext.Subscriptions.SubscriptionsConnectionsState;
            return  _state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateOrchestrator(_serverStore, TcpConnection.DatabaseContext, subId));
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


        public override async Task ProcessSubscriptionAsync()
        {
            StartShardSubscriptionWorkers();
            await MaintainConnectionWithClientWorkerAsync();
        }

        public override void FinishProcessing()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing sharded subscription."));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Finished processing sharded subscription '{SubscriptionId}' / from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for sharded database '{DatabaseName}' on '{_serverStore.NodeTag}'",
                ClientType = $"'client worker' with IP '{TcpConnection.TcpClient.Client.RemoteEndPoint}'",
                SubscriptionType = $"sharded subscription '{_options?.SubscriptionName}', id '{SubscriptionId}'"
            };
        }

        public override async Task ParseSubscriptionOptionsAsync()
        {
            await base.ParseSubscriptionOptionsAsync();

            // we want to limit the batch of each shard, to not hold too much memory if there are other batches while batch is proceed
            _options.MaxDocsPerBatch = Math.Max(Math.Min(_options.MaxDocsPerBatch / TcpConnection.DatabaseContext.ShardCount, _options.MaxDocsPerBatch), 1);
        }

        public override async Task ReportExceptionAsync(SubscriptionError error, Exception e)
        {
            try
            {
                await LogExceptionAndReportToClientAsync(ConnectionException ?? e);
            }
            catch
            {
                // ignored
            }
        }

        protected override Task OnClientAckAsync() => Task.CompletedTask;
        public override Task SendNoopAckAsync() => Task.CompletedTask;

        private void StartShardSubscriptionWorkers()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Create));

            for (int i = 0; i < TcpConnection.DatabaseContext.ShardCount; i++)
            {
                var re = TcpConnection.DatabaseContext.ShardExecutor.GetRequestExecutorAt(i);
                var shard = ShardHelper.ToShardName(TcpConnection.DatabaseContext.DatabaseName, i);
                var worker = CreateShardedWorkerHolder(shard, re, lastErrorDateTime: null);
                _shardWorkers.Add(shard, worker);
            }
        }

        private SubscriptionShardHolder CreateShardedWorkerHolder(string shard, RequestExecutor re, DateTime? lastErrorDateTime)
        {
            var options = _options.Clone();

            // we don't want to ensure that only one orchestrated connection handle the subscription
            options.Strategy = SubscriptionOpeningStrategy.TakeOver;
            options.WorkerId += $"/{ShardHelper.GetShardNumber(shard)}";

            var shardWorker = new ShardedSubscriptionWorker(options, shard, re, _state);

            var holder = new SubscriptionShardHolder(shardWorker, shardWorker.RunInternalAsync(CancellationTokenSource.Token), re)
            {
                LastErrorDateTime = lastErrorDateTime
            };

            return holder;
        }

        private async Task MaintainConnectionWithClientWorkerAsync()
        {
            var hasBatch = _state.HasNewDocuments.WaitAsync(CancellationTokenSource.Token);
            while (CancellationTokenSource.IsCancellationRequested == false)
            {
                var whenAny = await Task.WhenAny(TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(WaitForChangedDocumentsTimeoutInMs), CancellationTokenSource.Token),
                    hasBatch);
                CancellationTokenSource.Token.ThrowIfCancellationRequested();
                HandleBatchFromWorkersResult result;
                if (whenAny == hasBatch)
                {
                    hasBatch = _state.HasNewDocuments.WaitAsync(CancellationTokenSource.Token);
                    result = await TryHandleBatchFromWorkersAndCheckReconnectAsync(redirectBatch: true);
                }
                else
                {
                    await SendHeartBeatAsync("Waited for 3000ms for batch from shard workers");
                    result = await TryHandleBatchFromWorkersAndCheckReconnectAsync(redirectBatch: false);
                }

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

        private async Task<HandleBatchFromWorkersResult> TryHandleBatchFromWorkersAndCheckReconnectAsync(bool redirectBatch)
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

                if (shardHolder.PullingTask.IsCompleted == false)
                {
                    if (redirectBatch == false)
                        continue;

                    var batch = shardHolder.Worker.PublishedShardBatchItem;
                    if (batch == null)
                        continue;

                    await RedirectBatchAndConfirmAsync(batch, shard);
                    Interlocked.CompareExchange(ref shardHolder.Worker.PublishedShardBatchItem, null, batch);

                    continue;
                }

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

                (bool shouldTryToReconnect, _) = worker.Value.Worker.CheckIfShouldReconnectWorker(ex, CancellationTokenSource, assertLastConnectionFailure: null,
                    onUnexpectedSubscriptionError: null, throwOnRedirectNodeNotFound: false);
                if (shouldTryToReconnect)
                {
                    // we have at least one worker to try to reconnect
                    stopping = false;
                }
            }

            return stopping;
        }

        private async Task RedirectBatchAndConfirmAsync(ShardedSubscriptionWorker.PublishedShardBatch batch, string shard)
        {
            try
            {
                // Send to client
                var sentAnythingToClient = await WriteBatchToClientAndAckAsync(batch, shard);

                // let sharded subscription worker know that we sent the batch to the client and received an ack request from it
                batch.SendBatchToClientTcs.SetResult();

                // wait for sharded subscription worker to send ack to the shard subscription connection
                // and receive the confirm from the shard subscription connection
                await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;

                if (sentAnythingToClient)
                    // send confirm to client and continue processing
                    await SendConfirmToClientAsync();
            }
            catch (Exception e)
            {
                // need to fail the shard subscription worker
                batch.SendBatchToClientTcs.TrySetException(e);
                throw;
            }
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

        private Task<SubscriptionConnectionClientMessage> _replyFromClientTask;

        private async Task<bool> WriteBatchToClientAndAckAsync(ShardedSubscriptionWorker.PublishedShardBatch batch, string shardedDatabaseName)
        {
            _replyFromClientTask ??= GetReplyFromClientAsync();
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to send documents."));
            int docsToFlush = 0;
            string lastReceivedChangeVector = null;

            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (context.CheckoutMemoryStream(out var buffer))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, buffer))
            {
                foreach (var doc in batch._batchFromServer.Messages)
                {
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();
                    (BlittableJsonReaderObject metadata, LazyStringValue id, string changeVector) = BatchFromServer.GetMetadataFromBlittable(doc.Data);

                    if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                    {
                        await SendHeartBeatAsync("Skipping docs for more than 1000ms without sending any data");
                        sendingCurrentBatchStopwatch.Restart();
                    }

                    lastReceivedChangeVector = changeVector;
                    SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, doc.Data, metadata, doc.Exception, id, null, null, null);
                    docsToFlush++;

                    if (await SubscriptionConnection.FlushBatchIfNeededAsync(sendingCurrentBatchStopwatch, SubscriptionId, writer, buffer, TcpConnection, metrics: null,
                            _logger, docsToFlush, CancellationTokenSource.Token) == false)
                        continue;

                    docsToFlush = 0;
                    sendingCurrentBatchStopwatch.Restart();
                }

                if (lastReceivedChangeVector == null)
                {
                    // nothing to send
                    await SendHeartBeatAsync($"Shard {shardedDatabaseName} found nothing to send");
                    AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Shard {shardedDatabaseName} found nothing to send"));
                    return false;
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Major,
                    "https://issues.hibernatingrhinos.com/issue/RavenDB-16279");
                SubscriptionConnection.WriteEndOfBatch(writer);

                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Flushing docs collected from shard '{shardedDatabaseName}'"));
                await SubscriptionConnection.FlushDocsToClientAsync(SubscriptionId, writer, buffer, TcpConnection, metrics: null, _logger, docsToFlush, endOfBatch: true,
                    CancellationTokenSource.Token);
            }

            batch.LastSentChangeVectorInBatch = lastReceivedChangeVector;
            _replyFromClientTask = await WaitForClientAck(_replyFromClientTask);
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                $"Shard '{shardedDatabaseName}' got ack from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
            return true;
        }

        internal async Task SendConfirmToClientAsync()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                $"Shard subscription connection '{SubscriptionId}' send confirm to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
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
