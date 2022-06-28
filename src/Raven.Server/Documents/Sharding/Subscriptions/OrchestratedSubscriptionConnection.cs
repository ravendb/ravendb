// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public class OrchestratedSubscriptionConnection : SubscriptionConnectionBase
    {
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
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to process subscription"));
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Starting processing documents for subscription {SubscriptionId} received from {ClientUri}");
            }

            _replyFromClientTask = GetReplyFromClientAsync();

            while (CancellationTokenSource.IsCancellationRequested == false)
            {
                var inProgressBatchStats = Stats.CreateInProgressBatchStats();

                using (var batchScope = inProgressBatchStats.CreateScope())
                {
                    try
                    {
                        var sendingCurrentBatchStopwatch = Stopwatch.StartNew();

                        /*var anyDocumentsSentInCurrentIteration =
                            await TrySendingBatchToClient(docsContext, sendingCurrentBatchStopwatch, batchScope, inProgressBatchStats);
                            */

                        var anyDocumentsSentInCurrentIteration = await TrySendingBatchToClient();

                        if (anyDocumentsSentInCurrentIteration == false)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"Did not find any documents to send for subscription {Options.SubscriptionName}");
                            }

                            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                                $"Acknowledging docs processing progress without sending any documents to client."));

                            Stats.UpdateBatchPerformanceStats(0, false);

                            if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                                await SendHeartBeatAsync("Didn't find any documents to send and more then 1000ms passed");

                            AssertCloseWhenNoDocsLeft();

                            if (await WaitForChangedDocsAsync(_state, _replyFromClientTask))
                                continue;
                        }

                        /*
                        using (batchScope.For(SubscriptionOperationScope.BatchWaitForAcknowledge))
                        {
                            _replyFromClientTask = await WaitForClientAck(_replyFromClientTask);
                        }*/

                        // var last = Stats.UpdateBatchPerformanceStats(batchScope.GetBatchSize());
                        // TcpConnection.DocumentDatabase.SubscriptionStorage.RaiseNotificationForBatchEnded(_options.SubscriptionName, last);

                    }
                    catch (Exception e)
                    {
                        batchScope.RecordException(e.ToString());
                        throw;
                    }
                }
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        public override void FinishProcessing()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing sharded subscription."));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Finished processing sharded subscription '{SubscriptionId}' / from client '{ClientUri}'.");
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for sharded database '{DatabaseName}' on '{_serverStore.NodeTag}'",
                ClientType = $"'client worker' with IP '{ClientUri}'",
                SubscriptionType = $"sharded subscription '{_options?.SubscriptionName}', id '{SubscriptionId}'"
            };
        }

        protected override Task OnClientAckAsync() => Task.CompletedTask;
        public override Task SendNoopAckAsync() => Task.CompletedTask;

        private async Task<bool> TrySendingBatchToClient()
        {
            if (await _state.WaitForSubscriptionActiveLock(300) == false)
                return false;
            
            var released = false;

            try
            {
                if (_state.Batches.TryTake(out var batch, TimeSpan.Zero) == false)
                    return false;

                try
                {
                    var sentAnythingToClient = await WriteBatchToClientAndAckAsync(batch);

                    _state.ReleaseSubscriptionActiveLock();
                    released = true;

                    if (sentAnythingToClient)
                    {
                        _replyFromClientTask = await WaitForClientAck(_replyFromClientTask);
                        AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                            $"Shard '{batch.ShardName}' got ack from client '{ClientUri}'."));
                    }

                    // let sharded subscription worker know that we sent the batch to the client and received an ack request from it
                    batch.SendBatchToClientTcs.TrySetResult();

                    // wait for sharded subscription worker to send ack to the shard subscription connection
                    // and receive the confirm from the shard subscription connection
                    await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;

                    if (sentAnythingToClient)
                        // send confirm to client and continue processing
                        await SendConfirmToClientAsync();

                    return true;
                }
                catch (Exception e)
                {
                    // need to fail the shard subscription worker
                    batch.SendBatchToClientTcs.TrySetException(e);
                    throw;
                }
            }
            finally
            {
                if (released == false)
                    _state.ReleaseSubscriptionActiveLock();
            }
        }
      
        private Task<SubscriptionConnectionClientMessage> _replyFromClientTask;

        private async Task<bool> WriteBatchToClientAndAckAsync(ShardedSubscriptionBatch batch)
        {
            var shardedDatabaseName = batch.ShardName;

            try
            {
                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to send documents."));
                int docsToFlush = 0;
                string lastReceivedChangeVector = null;

                var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
                using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.CheckoutMemoryStream(out var buffer))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, buffer))
                {
                    foreach (var doc in batch.Items)
                    {
                        var data = doc.RawResult;
                        
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();
                        (BlittableJsonReaderObject metadata, LazyStringValue id, string changeVector) = BatchFromServer.GetMetadataFromBlittable(data);

                        if (sendingCurrentBatchStopwatch.ElapsedMilliseconds > 1000)
                        {
                            await SendHeartBeatAsync("Skipping docs for more than 1000ms without sending any data");
                            sendingCurrentBatchStopwatch.Restart();
                        }

                        lastReceivedChangeVector = changeVector;
                        SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, data, metadata, doc.ExceptionMessage, id, null, null, null);
                        docsToFlush++;

                        if (await SubscriptionConnection.FlushBatchIfNeededAsync(sendingCurrentBatchStopwatch, SubscriptionId, writer, buffer, TcpConnection,
                                metrics: null,
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
                    await SubscriptionConnection.FlushDocsToClientAsync(SubscriptionId, writer, buffer, TcpConnection, metrics: null, _logger, docsToFlush,
                        endOfBatch: true,
                        CancellationTokenSource.Token);
                }
                
                batch.LastSentChangeVectorInBatch = lastReceivedChangeVector;
                return true;
            }
            finally
            {
            }
        }

        internal async Task SendConfirmToClientAsync()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info,
                $"Shard subscription connection '{SubscriptionId}' send confirm to client '{ClientUri}'."));
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
        }
    }
}
