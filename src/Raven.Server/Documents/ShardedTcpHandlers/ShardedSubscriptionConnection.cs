// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private readonly MemoryStream _buffer = new MemoryStream();
        private readonly Dictionary<string, SubscriptionShardHolder> _shardWorkers = new Dictionary<string, SubscriptionShardHolder>();
        private readonly TimeSpan _rejectedReconnectTimeout = TimeSpan.FromMinutes(15); // TODO: egor create configuration
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.ShardedContext.DatabaseName, CancellationTokenSource.CreateLinkedTokenSource(tcpConnection.ShardedContext.DatabaseShutdown))
        {
        }

        public async Task Run()
        {
            try
            {
                CreateStatsScope();

                try
                {
                    await StartShardSubscriptionWorkers();
                    await MaintainConnectionWithClientWorker();
                }
                catch (SubscriptionInvalidStateException)
                {
                    _pendingConnectionScope.Dispose();
                    throw;
                }
            }
            catch (Exception e)
            {
                await RecordExceptionAndReportToClient(ConnectionException ?? e, sharded: true);
            }
            finally
            {
                AddToStatusDescription("Finished processing sharded subscription.");
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Finished processing sharded subscription '{SubscriptionId}' / from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");

                Dispose();
            }
        }

        public static void SendShardedSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress($"ShardedSubscription_{tcpConnectionOptions.ShardedContext.DatabaseName}");
            try
            {
                var connection = new ShardedSubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                Task.Run(async () => await connection.Run());
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();
                throw;
            }
        }

        public override SubscriptionConnectionState OpenSubscription()
        {
            return TcpConnection.ShardedContext.ShardedSubscriptionStorage.OpenSubscription(this);
        }

        private async Task StartShardSubscriptionWorkers()
        {
            await ParseSubscriptionOptionsAsync();
            AddToStatusDescription($"A connection for sharded subscription '{_options.SubscriptionName}' with id '{SubscriptionId}' for database '{TcpConnection.ShardedContext.DatabaseName}' was received from remote IP {TcpConnection.TcpClient.Client.RemoteEndPoint}");
            SubscriptionState = await TcpConnection.ShardedContext.ShardedSubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);

            //TODO: egor parse subscription query and check supported features ? if yes then skip the check on shards ? if no its get checked on shards anyway

            await TryConnectSubscription();
            try
            {
                _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

                for (int i = 0; i < TcpConnection.ShardedContext.Count; i++)
                {
                    var re = TcpConnection.ShardedContext.RequestExecutors[i];
                    if (re.Topology == null)
                        await re.WaitForTopologyUpdate(re._firstTopologyUpdate);

                    var shard = TcpConnection.ShardedContext.GetShardedDatabaseName(i);

                    // we want to limit the batch of each shard, to not hold too much memory if there are other batches while batch is proceed
                    if (_options.MaxDocsPerBatch == 4096) // this is default value
                    {
                        _options.MaxDocsPerBatch = 1024; //TODO: egor add configuration for shard worker MaxDocsPerBatch
                        _options.MaxErroneousPeriod = TimeSpan.MaxValue; //TODO: egor we have reconnect with timeout now
                    }

                    var shardWorker = new ShardedSubscriptionWorker(_options, shard, re, SendConfirmToClientUnderLock, ReportExceptionToAndStopProcessing, WriteBatchToClientAndAck);

                    var worker = new SubscriptionShardHolder
                    {
                        Worker = shardWorker,
                        PullingTask = shardWorker.RunInternal(CancellationTokenSource.Token),
                        RequestExecutor = re
                    };

                    _shardWorkers.Add(shard, worker);
                }

                var exceptions = new List<Exception>();
                foreach (var worker in _shardWorkers)
                {
                    if (worker.Value.PullingTask.IsFaulted)
                    {
                        exceptions.Add(worker.Value.PullingTask.Exception);
                        worker.Value.LastErrorDateTime = DateTime.Now;
                    }
                }

                if (exceptions.Count == _shardWorkers.Count)
                    throw new AggregateException($"Cannot start sharded subscription '{_options.SubscriptionName}' with id '{SubscriptionId}' for database '{TcpConnection.ShardedContext.DatabaseName}' because all shard workers failed.", exceptions);

                await WriteJsonAsync(new DynamicJsonValue
                {
                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                    [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                }, TcpConnection);
            }
            catch
            {
                DisposeOnDisconnect.Dispose();
                throw;
            }
        }

        private async Task MaintainConnectionWithClientWorker()
        {
            using (DisposeOnDisconnect)
            {
                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    await TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000), CancellationTokenSource.Token);
                    CancellationTokenSource.Token.ThrowIfCancellationRequested();

                    if (await _lock.WaitAsync(0))
                    {
                        // no batches are currently being sent, we have to ping the client
                        try
                        {
                            await SendHeartBeat("Waited for 3000ms for batch from shard workers");
                        }
                        finally
                        {
                            _lock.Release();
                        }
                    }

                    Dictionary<string, Exception> exceptions = new Dictionary<string, Exception>();
                    List<string> shardsToReconnect = new List<string>();
                    var stopping = false;
                    foreach ((string shard, SubscriptionShardHolder shardHolder) in _shardWorkers)
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        if (shardHolder.PullingTask.IsFaulted == false)
                            continue;

                        exceptions.Add(shard, shardHolder.PullingTask.Exception);

                        if (stopping)
                            continue;

                        if (shardHolder.LastErrorDateTime.HasValue == false)
                        {
                            shardHolder.LastErrorDateTime = DateTime.Now;
                            shardsToReconnect.Add(shard);
                        }
                        else if (DateTime.Now - shardHolder.LastErrorDateTime.Value < _rejectedReconnectTimeout)
                        {
                            shardsToReconnect.Add(shard);
                        }
                        else
                        {
                            // we are stopping this subscription
                            stopping = true;
                        }
                    }

                    if (stopping)
                        throw new AggregateException($"Stopping sharded subscription '{_options.SubscriptionName}' with id '{SubscriptionId}' for database '{TcpConnection.ShardedContext.DatabaseName}' because shard {string.Join(", ", exceptions.Keys)} workers failed.", exceptions.Values);

                    // try reconnect faulted workers, this will happen every 3 sec
                    foreach (var shard in shardsToReconnect)
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        if (_shardWorkers.ContainsKey(shard) == false)
                            continue;

                        using (var old = _shardWorkers[shard].Worker)
                        {
                            _shardWorkers[shard].Worker = new ShardedSubscriptionWorker(_options, shard, _shardWorkers[shard].RequestExecutor, SendConfirmToClientUnderLock, ReportExceptionToAndStopProcessing, WriteBatchToClientAndAck);
                            var t = _shardWorkers[shard].Worker.RunInternal(CancellationTokenSource.Token);
                            _shardWorkers[shard].PullingTask = t;
                        }
                    }
                }

                CancellationTokenSource.Token.ThrowIfCancellationRequested();
            }
        }

        internal async Task SendConfirmToClientUnderLock()
        {
            Debug.Assert(_lock.CurrentCount == 0, "SendConfirmToClientUnderLock is NOT under lock");

            try
            {
                AddToStatusDescription($"Shard subscription connection '{SubscriptionId}' send confirm to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
                await WriteJsonAsync(new DynamicJsonValue { [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.Confirm) }, TcpConnection);
            }
            finally
            {
                _lock.Release();
            }
        }

        internal async Task ReportExceptionToAndStopProcessing(Exception ex)
        {
            try
            {
                AddToStatusDescription($"Stopping subscription connection '{SubscriptionId}' and report exception to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'. Exception: {ex}");
                await ReportExceptionToClient(ex);
            }
            catch
            {
                // ignore
            }
            finally
            {

                try
                {
                    CancellationTokenSource.Cancel();
                }
                catch
                {
                    // ignore
                }
            }
        }

        private async Task WriteBatchToClientAndAck(SubscriptionBatch<dynamic> batch)
        {
            // get lock, we can process only one batch in a time
            await _lock.WaitAsync(CancellationTokenSource.Token).ConfigureAwait(false);

            try
            {
                var replyFromClientTask = GetReplyFromClientAsync();
                AddToStatusDescription($"Starting to send documents from shard '{batch._shardWorker._dbName}' to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
                int docsToFlush = 0;
                var sendingCurrentBatchStopwatch = Stopwatch.StartNew();
                using (TcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, _buffer))
                {
                    foreach (var doc in batch.Items)
                    {
                        CancellationTokenSource.Token.ThrowIfCancellationRequested();

                        SubscriptionConnection.WriteDocumentOrException(context, writer, document: null, doc.RawResult, doc.RawMetadata, doc.ExceptionMessage, doc.Id, null, null, null);
                        docsToFlush++;

                        if (await SubscriptionConnection.FlushBatchIfNeeded(sendingCurrentBatchStopwatch, SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, CancellationTokenSource.Token) == false)
                            continue;

                        docsToFlush = 0;
                        sendingCurrentBatchStopwatch.Restart();
                    }

                    //TODO: egor Handle includes

                    SubscriptionConnection.WriteEndOfBatch(writer);

                    AddToStatusDescription($"Flushing docs collected from shard '{batch._shardWorker._dbName}' to client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
                    await SubscriptionConnection.FlushDocsToClient(SubscriptionId, writer, _buffer, TcpConnection, Stats, _logger, docsToFlush, endOfBatch: true, CancellationTokenSource.Token);
                }

                await WaitForClientAck(replyFromClientTask, null, sharded: true);
                AddToStatusDescription($"Shard '{batch._shardWorker._dbName}' got ack from client '{TcpConnection.TcpClient.Client.RemoteEndPoint}'.");
            }
            catch
            {
                // release lock in case of exception
                _lock.Release();
                throw;
            }
        }

        internal override Task<string> OnClientAck(string subscriptionChangeVectorBeforeCurrentBatch)
        {
            return Task.FromResult<string>(null);
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

            _buffer.Dispose();
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
