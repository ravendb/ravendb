// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private readonly Dictionary<string, SubscriptionShardHolder> _shardWorkers = new Dictionary<string, SubscriptionShardHolder>();
        public static ConcurrentDictionary<string, ShardedSubscriptionConnection> Connections = new ConcurrentDictionary<string, ShardedSubscriptionConnection>();

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.ShardedContext.DatabaseName, CancellationTokenSource.CreateLinkedTokenSource(tcpConnection.ShardedContext.DatabaseShutdown))
        {
        }

        public async Task Run()
        {
            try
            {
                await ParseSubscriptionOptionsAsync();
                await StartShardSubscriptionWorkers();
                await MaintainConnectionWithClientWorker();
            }
            catch (Exception e)
            {
                await ReportException(SubscriptionError.Error, ConnectionException ?? e);
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

        private new async Task ParseSubscriptionOptionsAsync()
        {
            await base.ParseSubscriptionOptionsAsync();

            // we want to limit the batch of each shard, to not hold too much memory if there are other batches while batch is proceed
            _options.MaxDocsPerBatch = Math.Min(_options.MaxDocsPerBatch / TcpConnection.ShardedContext.ShardCount, _options.MaxDocsPerBatch);
            _options.MaxDocsPerBatch = Math.Max(1, _options.MaxDocsPerBatch);

            // add to connections
            if (Connections.TryAdd(_options.SubscriptionName, this) == false)
            {
                throw new InvalidOperationException($"Sharded subscription '{_options.SubscriptionName}' on '{Database}' already exists!");
            } 
        }

        protected override async Task ReportException(SubscriptionError error, Exception e)
        {
            try
            {
                await LogExceptionAndReportToClient(ConnectionException ?? e);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        protected override Task OnClientAck()
        {
            return Task.CompletedTask;
        }

        public static void SendShardedSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress($"ShardedSubscription_{tcpConnectionOptions.ShardedContext.DatabaseName}");
            try
            {
                var connection = new ShardedSubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                _ = connection.Run();
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();
                throw;
            }
        }

        private async Task StartShardSubscriptionWorkers()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Create));

            for (int i = 0; i < TcpConnection.ShardedContext.ShardCount; i++)
            {
                var re = TcpConnection.ShardedContext.RequestExecutors[i];
                if (re.Topology == null)
                    await re.WaitForTopologyUpdate(re._firstTopologyUpdate);

                var shard = TcpConnection.ShardedContext.GetShardedDatabaseName(i);

                var shardWorker = new ShardedSubscriptionWorker(_options, shard, re);

                var worker = new SubscriptionShardHolder
                {
                    Worker = shardWorker,
                    PullingTask = shardWorker.RunInternal(CancellationTokenSource.Token),
                    RequestExecutor = re
                };

                _shardWorkers.Add(shard, worker);
            }

            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            });
        }

        private async Task MaintainConnectionWithClientWorker()
        {
            var exceptions = new Dictionary<string, Exception>();
            var shardsToReconnect = new List<string>();
            var heartbeatSp = Stopwatch.StartNew();
            var reconnectSp = Stopwatch.StartNew();
            var didBatch = false;
            while (CancellationTokenSource.IsCancellationRequested == false)
            {
                exceptions.Clear();
                shardsToReconnect.Clear();
                if (didBatch == false)
                    await Task.Delay(500);

                if (heartbeatSp.ElapsedMilliseconds >= 3000)
                {
                    await SendHeartBeat("Waited for 3000ms for batch from shard workers");
                    heartbeatSp.Restart();
                }
                didBatch = false;
                var stopping = false;
                foreach ((string shard, SubscriptionShardHolder shardHolder) in _shardWorkers)
                {
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (shardHolder.PullingTask.IsFaulted == false)
                    {
                        ShardedSubscriptionWorker.PublishedBatch batch = shardHolder.Worker.PublishedBatchItem;
                        if (batch == null)
                            continue;

                        didBatch = true;
                        // Send to client
                        try
                        {
                            await WriteBatchToClientAndAck(batch, shard);
                            batch.SendBatchToClientTcs.SetResult();
                        }
                        catch (Exception e)
                        {
                            // need to fail the shard subscription worker
                            batch.SendBatchToClientTcs.TrySetException(e);
                            throw;
                        }
                        await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;

                        // send confirm to client and continue processing
                        await SendConfirmToClient();
                        continue;
                    }

                    exceptions.Add(shard, shardHolder.PullingTask.Exception);

                    if (stopping)
                        continue;

                    shardsToReconnect.Add(shard);

                    if (shardHolder.LastErrorDateTime.HasValue == false)
                    {
                        shardHolder.LastErrorDateTime = DateTime.UtcNow;
                        continue;
                    }

                    if (DateTime.UtcNow - shardHolder.LastErrorDateTime.Value <= _options.ShardedConnectionMaxErroneousPeriod)
                        continue;

                    // we are stopping this subscription
                    stopping = true;
                }

                if (exceptions.Count == _shardWorkers.Count && stopping == false)
                {
                    stopping = true;
                    foreach (var worker in _shardWorkers)
                    {
                        if (exceptions.TryGetValue(worker.Key, out var ex) == false)
                            throw new InvalidOperationException($"Expected to get exception for shard worker {worker.Key} but could not find.");

                        (bool shouldTryToReconnect, _) = worker.Value.Worker.CheckIfShouldReconnectWorker(ex, CancellationTokenSource, null, null, throwOnRedirectNodeNotFound: false);
                        if (shouldTryToReconnect)
                        {
                            // we have at least one worker to try to reconnect
                            stopping = false;
                        }
                    }
                }

                if (stopping)
                {
                    throw new AggregateException($"Stopping sharded subscription '{_options.SubscriptionName}' with id '{SubscriptionId}' for database '{TcpConnection.ShardedContext.DatabaseName}' because shard {string.Join(", ", exceptions.Keys)} workers failed.", exceptions.Values);
                }

                if (reconnectSp.ElapsedMilliseconds < 3000)
                    continue;

                // try reconnect faulted workers, this will happen every 3 sec
                foreach (var shard in shardsToReconnect)
                {
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (_shardWorkers.ContainsKey(shard) == false)
                        continue;

                    using (var old = _shardWorkers[shard].Worker)
                    {
                        _shardWorkers[shard].Worker = new ShardedSubscriptionWorker(_options, shard, _shardWorkers[shard].RequestExecutor);
                        var t = _shardWorkers[shard].Worker.RunInternal(CancellationTokenSource.Token);
                        _shardWorkers[shard].PullingTask = t;
                    }
                }

                reconnectSp.Restart();
            }

            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        private async Task WriteBatchToClientAndAck(ShardedSubscriptionWorker.PublishedBatch batch, string shard)
        {
            var replyFromClientTask = GetReplyFromClientAsync();
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Starting to send documents."));
            int docsToFlush = 0;
            string lastReceivedChangeVector = null;

            var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
            using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var buffer = new MemoryStream())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, buffer))
            {
                foreach (var doc in batch._batchFromServer.Messages)
                {
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();
                    (BlittableJsonReaderObject metadata, string id, string changeVector) = BatchFromServer.GetMetadataFromBlittable(doc.Data);
                    lastReceivedChangeVector = changeVector;
                    SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, doc.Data, metadata, doc.Exception, id, null, null, null);
                    docsToFlush++;

                    if (await SubscriptionConnection.FlushBatchIfNeeded(sendingCurrentBatchStopwatch, SubscriptionId, writer, buffer, TcpConnection, stats: null, _logger, docsToFlush, CancellationTokenSource.Token) == false)
                        continue;

                    docsToFlush = 0;
                    sendingCurrentBatchStopwatch.Restart();
                }

                //TODO: egor https://issues.hibernatingrhinos.com/issue/RavenDB-16279

                SubscriptionConnection.WriteEndOfBatch(writer);

                AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Flushing docs collected from shard '{shard}'"));
                await SubscriptionConnection.FlushDocsToClient(SubscriptionId, writer, buffer, TcpConnection, stats: null, _logger, docsToFlush, endOfBatch: true, CancellationTokenSource.Token);
            }

            batch.LastSentChangeVectorInBatch = lastReceivedChangeVector;
            await WaitForClientAck(replyFromClientTask, sharded: true);
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Shard '{shard}' got ack from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
        }

        internal async Task SendConfirmToClient()
        {

            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, $"Shard subscription connection '{SubscriptionId}' send confirm to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'."));
            await WriteJsonAsync(new DynamicJsonValue { [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm) });
        }

        public new void Dispose()
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
            public ShardedSubscriptionWorker Worker;
            public Task PullingTask;
            public DateTime? LastErrorDateTime;
            public RequestExecutor RequestExecutor;

            public void Dispose()
            {
                Worker.Dispose();
            }
        }
    }
}
