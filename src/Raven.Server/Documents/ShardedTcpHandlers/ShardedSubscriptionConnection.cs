// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
// ReSharper disable AccessToDisposedClosure

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private List<SubscriptionServerWorker> _workers = new List<SubscriptionServerWorker>();
        private HashSet<string> _rejectedShards = new HashSet<string>();
        private readonly TimeSpan _rejectedReconnectTimeout = TimeSpan.FromMinutes(15); // TODO: egor create configuration
        private bool _isDisposed;

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnectionOptions, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnectionOptions, serverStore, buffer, tcpConnectionDisposable)
        {
        }

        //TODO: egor handle double code with InitAsync and AssertConnection
        public async Task TryReconnectRejectedShards()
        {
            var sp = new Stopwatch();
            while (CancellationTokenSource.IsCancellationRequested == false)
            {
                if (_rejectedShards.Count == 0)
                {
                    await Task.Delay(1000);
                    if(sp.IsRunning)
                        sp.Reset();

                    continue;
                }

                var rejectedShards = _rejectedShards;
                if (CancellationTokenSource.IsCancellationRequested)
                    break;

                if (sp.IsRunning == false)
                    sp.Start();
      
                if (sp.Elapsed >= _rejectedReconnectTimeout)
                    throw new SubscriptionConnectionDownException($"Could not reconnect shards '{string.Join(",", rejectedShards)}' stopping sharded subscription '{Options.SubscriptionName}'.");

                var workers = new List<SubscriptionServerWorker>();
                var skippedNodes = new Dictionary<string, List<string>>();
                foreach (var shard in rejectedShards)
                {
                    skippedNodes[shard] = new List<string>();
                    var shardId = ShardHelper.TryGetShardIndex(shard);
                    var re = TcpConnection.ShardedContext.RequestExecutors[shardId];
                    if (re.Topology == null)
                    {
                        await re.WaitForTopologyUpdate(re._firstTopologyUpdate);
                        if (re.Topology == null)
                        {
                            throw new Exception("ShouldNot happen and likely a bug.!!");
                        }
                    }

                    SubscriptionServerWorker worker = null;
                    List<Exception> exceptions = new List<Exception>();
                    foreach (var node in re.Topology.Nodes)
                    {
                        try
                        {
                            worker = await GetShardedSubscriptionServerWorker(shard, re, node.ClusterTag);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            skippedNodes[shard].Add(node.ClusterTag);
                            continue;
                        }

                        workers.Add(worker);
                        break;
                    }
                }

                if (workers.Count == 0)
                    continue;

                foreach (var worker in workers)
                {
                    await SendOptionToServerWorker(worker);
                }

                if (CancellationTokenSource.IsCancellationRequested)
                    break;

                await AssertConnectionStatusForRejectedConnection(workers, skippedNodes, sp);
            }
        }

        //TODO: egor handle double code with InitAsync and AssertConnection
        private async Task AssertConnectionStatusForRejectedConnection(ICollection<SubscriptionServerWorker> workers, IReadOnlyDictionary<string, List<string>> skippedNodes, Stopwatch stopwatch)
        {
            while (workers.Count != 0 && CancellationTokenSource.IsCancellationRequested == false)
            {
                if (stopwatch.Elapsed >= _rejectedReconnectTimeout)
                    throw new SubscriptionConnectionDownException($"Could not reconnect shards '{string.Join(",", _rejectedShards)}' stopping sharded subscription '{Options.SubscriptionName}'.");

                var localList = new List<SubscriptionServerWorker>(workers);
                foreach (var worker in localList)
                {
                    // try assert connection for each worker
                    using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (context.GetMemoryBuffer(out var buffer))
                    {
                        var receivedMessage = await ReadNextObject(context, worker, buffer);
                        if (receivedMessage == null || CancellationTokenSource.IsCancellationRequested)
                            throw new SubscriptionClosedException($"Subscription '{SubscriptionId}' was closed");

                        if (receivedMessage.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
                        {
                            using (var bjro = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(receivedMessage, context))
                            {
                                await WriteBlittableAsync(bjro, TcpConnection);
                            }

                            throw new Exception($"Server returned illegal type message when expecting connection status, was: {receivedMessage.Type}, Exception: {receivedMessage.Exception}");
                        }

                        if (receivedMessage.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                        {
                            _workers.Add(worker);
                            workers.Remove(worker);
                            _rejectedShards.Remove(worker.Database);
                        }
                        else if (receivedMessage.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Redirect)
                        {
                            // add current node tag which got redirect
                            var database = worker.Database;
                            skippedNodes[database].Add(worker.NodeTag);
                            workers.Remove(worker);
                            worker.Dispose();

                            var re = TcpConnection.ShardedContext.RequestExecutors[ShardHelper.TryGetShardIndex(database)];
                            List<Exception> exceptions = new List<Exception>();
                            SubscriptionServerWorker newWorker = null;
                            var appropriateNode = receivedMessage.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                            if (appropriateNode == null)
                            {
                                while (skippedNodes[database].Count < re.Topology.Nodes.Count)
                                {
                                    // select node which is not in skipped list
                                    var selectedNode = re.Topology.Nodes.FirstOrDefault(x => skippedNodes[database].Contains(x.ClusterTag) == false);

                                    if (selectedNode == null)
                                    {
                                        break;
                                    }
               
                                    try
                                    {
                                        newWorker = await GetShardedSubscriptionServerWorker(database, re, selectedNode.ClusterTag);
                                        await SendOptionToServerWorker(newWorker);
                                    }
                                    catch (Exception e)
                                    {
                                        exceptions.Add(e);
                                        skippedNodes[database].Add(selectedNode.ClusterTag);
                                    }

                                    if (newWorker != null)
                                    {
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                if (re.Topology.Nodes.Select(x => x.ClusterTag).Contains(appropriateNode) == false)
                                {
                                    throw new InvalidOperationException($"Shard '{database}' topology '{string.Join(", ", re.Topology.Nodes)}' doesn't contain the subscription's appropriate node '{appropriateNode}' should not happen and likely a bug.");
                                }

                                try
                                {
                                    newWorker = await GetShardedSubscriptionServerWorker(database, re, appropriateNode);
                                    await SendOptionToServerWorker(newWorker);
                                }
                                catch (Exception e)
                                {
                                    exceptions.Add(e);
                                    skippedNodes[database].Add(appropriateNode);
                                    appropriateNode = null;
                                }

                                if (newWorker == null)
                                {
                                    while (skippedNodes[database].Count < re.Topology.Nodes.Count)
                                    {
                                        // select node which is not in skipped list
                                        var selectedNode = re.Topology.Nodes.FirstOrDefault(x => skippedNodes[database].Contains(x.ClusterTag) == false);
                                        if (selectedNode == null)
                                        {
                                            break;
                                        }

                                        try
                                        {
                                            newWorker = await GetShardedSubscriptionServerWorker(database, re, selectedNode.ClusterTag);
                                            await SendOptionToServerWorker(newWorker);
                                        }
                                        catch (Exception e)
                                        {
                                            exceptions.Add(e);
                                            skippedNodes[database].Add(selectedNode.ClusterTag);
                                        }

                                        if (newWorker != null)
                                        {
                                            break;
                                        }
                                    }
                                }
                            }


                            if (newWorker != null)
                            {
                                // we need to assert the connection again
                                workers.Add(newWorker);
                            }
                        }
                        else
                        {
                            throw new Exception($"TODO: egor, assert connection status failed for rejected node.");
                        }
                    }
                }
            }
        }

        public static void SendShardedSubscriptionDocuments(ServerStore server, TcpConnectionOptions tcpConnectionOptions, JsonOperationContext.MemoryBuffer buffer)
        {
            var remoteEndPoint = tcpConnectionOptions.TcpClient.Client.RemoteEndPoint;

            var tcpConnectionDisposable = tcpConnectionOptions.ConnectionProcessingInProgress("ShardedSubscription");
            try
            {
                var connection = new ShardedSubscriptionConnection(server, tcpConnectionOptions, tcpConnectionDisposable, buffer);
                try
                {
                    Task.Run(async () =>
                    {
                        using (tcpConnectionOptions)
                        using (tcpConnectionDisposable)
                        using (connection)
                        {
                            connection._lastConnectionStats = new SubscriptionConnectionStatsAggregator(_connectionStatsId, null);
                            connection._connectionScope = connection._lastConnectionStats.CreateScope();
                            connection._pendingConnectionScope = connection._connectionScope.For(SubscriptionOperationScope.ConnectionPending);
                            try
                            {
                                try
                                {
                                    // setup connect with timeout
                                    var t = Task.Run(() => connection.TryReconnectRejectedShards());
                                    // try connect subscription
                                    await connection.InitAsync();
                                    await connection.ProcessSubscriptionAsync();
                                }
                                catch (SubscriptionInvalidStateException)
                                {
                                    connection._pendingConnectionScope.Dispose();
                                    throw;
                                }

                            }
                            catch (Exception e)
                            {
                                connection._connectionScope.RecordException(e is SubscriptionInUseException ? SubscriptionError.ConnectionRejected : SubscriptionError.Error, e.Message);

                                var errorMessage = $"Failed to process sharded subscription {connection.SubscriptionId} / from client {remoteEndPoint}";
                                connection.AddToStatusDescription($"{errorMessage}. Sending response to client");
                                if (connection._logger.IsInfoEnabled)
                                    connection._logger.Info(errorMessage, e);

                                try
                                {
                                    switch (e)
                                    {
                                        case DatabaseDoesNotExistException _:
                                        case SubscriptionChangeVectorUpdateConcurrencyException _:
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, e, connection._logger);
                                            break;
                                        case RavenException _:
                                        case AllTopologyNodesDownException _:
                                            // could not connect shard, we report SubscriptionInvalidStateException so the worker throws
                                            //TODO: egor normal expcetion msg
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, new SubscriptionInvalidStateException("test", e), connection._logger);
                                            break;
                                        default:
                                            await ReportExceptionToClient(server, tcpConnectionOptions, connection, e, connection._logger);
                                            break;
                                    }
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            finally
                            {
                                connection.AddToStatusDescription("Finished processing sharded subscription.");
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info($"Finished processing sharded subscription '{connection.SubscriptionId}' / from client '{remoteEndPoint}'");
                                }
                            }
                        }
                    });
                }
                catch (Exception)
                {
                    connection?.Dispose();
                    throw;
                }
            }
            catch (Exception)
            {
                tcpConnectionDisposable?.Dispose();
                throw;
            }
        }

        private async Task InitAsync()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                (var options, long? id) = await ParseSubscriptionOptionsAsync(context, _serverStore, TcpConnection, _copiedBuffer.Buffer, TcpConnection.ShardedContext.DatabaseName, CancellationTokenSource.Token);
                _options = options;
                if (id.HasValue)
                    SubscriptionId = id.Value;
            }

            SubscriptionState = await TcpConnection.ShardedContext.ShardedSubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);
            var workers = new List<SubscriptionServerWorker>();
            var skippedNodes = new Dictionary<string, List<string>>();
            try
            {
                for (int i = 0; i < TcpConnection.ShardedContext.Count; i++)
                {
                    var re = TcpConnection.ShardedContext.RequestExecutors[i];
                    if (re.Topology == null)
                    {
                        await re.WaitForTopologyUpdate(re._firstTopologyUpdate);
                        if (re.Topology == null)
                        {
                            throw new Exception("ShouldNot happen and likely a  bug.!!");
                        }
                    }

                    SubscriptionServerWorker worker = null;
                    List<Exception> exceptions = new List<Exception>();
                    var shard = TcpConnection.ShardedContext.GetShardedDatabaseName(i);
                    skippedNodes[shard] = new List<string>();
                    foreach (var node in re.Topology.Nodes)
                    {
                        try
                        {
                            worker = await GetShardedSubscriptionServerWorker(shard, re, node.ClusterTag);
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            skippedNodes[shard].Add(node.ClusterTag);
                            continue;
                        }

                        workers.Add(worker);
                        break;
                    }

                    if (worker == null)
                    {
                        _rejectedShards.Add(shard);
                    }
                }

                await TryConnectSubscription();
                _pendingConnectionScope.Dispose();

                try
                {
                    _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

                    // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
                    SubscriptionState = await TcpConnection.ShardedContext.ShardedSubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);
                    foreach (var worker in workers)
                    {
                        await SendOptionToServerWorker(worker);
                    }
                }
                catch
                {
                    DisposeOnDisconnect.Dispose();
                    throw;
                }

                _connectionState = TcpConnection.ShardedContext.ShardedSubscriptionStorage.OpenSubscription(this);
                _connectionState.PendingConnections.Add(this);

                // Assert connection status
                await AssertConnectionStatus(workers, skippedNodes);

                await WriteJsonAsync(new DynamicJsonValue
                {
                    [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                    [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                }, TcpConnection);
            }
            catch (Exception e)
            {
                foreach (SubscriptionServerWorker worker in workers)
                {
                    worker.Dispose();
                }
                foreach (SubscriptionServerWorker worker in _workers)
                {
                    worker.Dispose();
                }
                throw;
            }

        }
        //TODO: egor handle double code with rejected connection
        private async Task AssertConnectionStatus(ICollection<SubscriptionServerWorker> workers, IReadOnlyDictionary<string, List<string>> skippedNodes)
        {
            var sp = Stopwatch.StartNew();
            var sp2 = Stopwatch.StartNew();
            // TODO: egor add normal timeout
            var timeout = int.MaxValue;
            while (workers.Count != 0 && sp.ElapsedMilliseconds < timeout)
            {
                var localList = new List<SubscriptionServerWorker>(workers);
                foreach (var worker in localList)
                {
                    // try assert connection for each worker
                    if (sp2.ElapsedMilliseconds >= 3000)
                    {
                        await SendHeartBeat("Waited for 3000ms for assert connection status to server workers");
                        sp2.Reset();
                    }

                    using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (context.GetMemoryBuffer(out var buffer))
                    {
                        var receivedMessage = await ReadNextObject(context, worker, buffer);
                        if (receivedMessage == null || CancellationTokenSource.IsCancellationRequested)
                            throw new SubscriptionClosedException($"Subscription '{SubscriptionId}' was closed");

                        if (receivedMessage.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
                        {
                            using (var bjro = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(receivedMessage, context))
                            {
                                await WriteBlittableAsync(bjro, TcpConnection);
                            }

                            throw new Exception($"Server returned illegal type message when expecting connection status, was: {receivedMessage.Type}, Exception: {receivedMessage.Exception}");
                        }

                        if (receivedMessage.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                        {
                            _workers.Add(worker);
                            workers.Remove(worker);
                        }
                        else if (receivedMessage.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Redirect)
                        {
                            // add current node tag which got redirected
                            var database = worker.Database;
                            skippedNodes[database].Add(worker.NodeTag);
                            workers.Remove(worker);
                            worker.Dispose();
                            var appropriateNode = receivedMessage.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                            var re = TcpConnection.ShardedContext.RequestExecutors[ShardHelper.TryGetShardIndex(database)];

                            List<Exception> exceptions = new List<Exception>();
                            SubscriptionServerWorker newWorker = null;
                            if (appropriateNode == null)
                            {
                                while (skippedNodes[database].Count < re.Topology.Nodes.Count)
                                {
                                    // select node which is not in skipped list
                                    var selectedNode = re.Topology.Nodes.FirstOrDefault(x => skippedNodes[database].Contains(x.ClusterTag) == false);
                                    if (selectedNode == null)
                                    {
                                        // we could not assert connection to worker and have no nodes left to connect => setup connect with timeout and continue to next worker.
                                        _rejectedShards.Add(database);
                                        break;
                                    }
                                 
                                    try
                                    {
                                        newWorker = await GetShardedSubscriptionServerWorker(database, re, selectedNode.ClusterTag);
                                        await SendOptionToServerWorker(newWorker);
                                    }
                                    catch (Exception e)
                                    {
                                        exceptions.Add(e);
                                        skippedNodes[database].Add(selectedNode.ClusterTag);
                                    }

                                    if (newWorker != null)
                                    {
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                if (re.Topology.Nodes.Select(x => x.ClusterTag).Contains(appropriateNode) == false)
                                {
                                    throw new InvalidOperationException($"Shard '{database}' topology '{string.Join(", ", re.Topology.Nodes)}' doesn't contain the subscription's appropriate node '{appropriateNode}' should not happen and likely a bug.");
                                }

                                try
                                {
                                    newWorker = await GetShardedSubscriptionServerWorker(database, re, appropriateNode);
                                    await SendOptionToServerWorker(newWorker);
                                }
                                catch (Exception e)
                                {
                                    exceptions.Add(e);
                                    skippedNodes[database].Add(appropriateNode);
                                }

                                if (newWorker == null)
                                {
                                    while (skippedNodes[database].Count < re.Topology.Nodes.Count)
                                    {
                                        // select node which is not in skipped list
                                        var selectedNode = re.Topology.Nodes.FirstOrDefault(x => skippedNodes[database].Contains(x.ClusterTag) == false);
                                        if (selectedNode == null)
                                        {
                                            // we could not assert connection to worker and have no nodes left to connect => setup connect with timeout and continue to next worker.
                                            _rejectedShards.Add(database);
                                            break;
                                        }

                                        try
                                        {
                                            newWorker = await GetShardedSubscriptionServerWorker(database, re, selectedNode.ClusterTag);
                                            await SendOptionToServerWorker(newWorker);
                                        }
                                        catch (Exception e)
                                        {
                                            exceptions.Add(e);
                                            skippedNodes[database].Add(selectedNode.ClusterTag);
                                        }

                                        if (newWorker != null)
                                        {
                                            break;
                                        }
                                    }
                                }
                            }

                            if (newWorker == null)
                            {
                                // we could not assert connection to worker, have no nodes left to connect and could not create new worker => setup connect with timeout and continue to next worker.
                                _rejectedShards.Add(database);
                            }
                            else
                            {
                                // we need to assert the connection again
                                workers.Add(newWorker);
                            }
                        }
                        else
                        {
                            throw new Exception($"TODO: egor, assert connection status failed in InitAsync");
                        }
                    }
                }
            }

            if (sp.ElapsedMilliseconds > timeout)
                throw new Exception("TODO: egor got timeout in assert connection status");
        }

        private async Task SendOptionToServerWorker(SubscriptionServerWorker worker)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var optionsJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(Options, context))
                {
                    await optionsJson.WriteJsonToAsync(worker.Stream, CancellationTokenSource.Token).ConfigureAwait(false);
                    await worker.Stream.FlushAsync(CancellationTokenSource.Token).ConfigureAwait(false);
                }
            }
        }

        private async Task ProcessSubscriptionAsync()
        {
            var replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);
            var i = 0;
            using (DisposeOnDisconnect)
            {
                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    var workers = _workers;
                    if (workers.Count == 0)
                    {
                        await Task.Delay(1000);
                        continue;
                    }

                    var currentWorker = workers[i++ % workers.Count];
                    currentWorker.PullingTask ??= ReadNextObject(currentWorker.Context, currentWorker, currentWorker.Buffer);

                    var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000));
                    var completedTask = await Task.WhenAny(replyFromClientTask, timeoutTask, currentWorker.PullingTask);

                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    if (completedTask == replyFromClientTask)
                    {
                        // forward to shards
                        var clientReply = await replyFromClientTask;
                        foreach (var worker in workers)
                        {
                            await SendClientReplyToServerWorker(worker, clientReply);
                        }

                        replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);

                        continue;
                    }

                    if (completedTask == timeoutTask)
                    {
                        // timeout send heartbeat to client worker
                        await SendHeartBeat("Waited for 3000ms for server workers");
                        continue;
                    }

                    var receivedMessage = await currentWorker.PullingTask;
                    if (receivedMessage.Type == SubscriptionConnectionServerMessage.MessageType.EndOfBatch)
                    {
                        // empty batch
                        currentWorker.PullingTask = null;
                        continue;
                    }

                    var hasMore = true;
                    while (CancellationTokenSource.IsCancellationRequested == false && hasMore)
                    {
                        hasMore = await HandleBatchFromServerWorkers(workers, receivedMessage, currentWorker);
                        if (currentWorker.IsDisposed)
                            break;

                        if (hasMore)
                        {
                            currentWorker.PullingTask = ReadNextObject(currentWorker.Context, currentWorker, currentWorker.Buffer);
                            receivedMessage = await currentWorker.PullingTask;  // TODO: egor add timeout?
                        }
                    }

                    if (currentWorker.IsDisposed)
                        continue;
                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    // finished batch lets wait for client worker ack
                    replyFromClientTask = await WaitForClientAck(workers, replyFromClientTask, currentWorker);
                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    // finished batch lets wait for server worker confirm
                    currentWorker.PullingTask = ReadNextObject(currentWorker.Context, currentWorker, currentWorker.Buffer);
                    receivedMessage = await currentWorker.PullingTask;  // TODO: egor add timeout?

                    await HandleServerMessageFromServerWorkers(workers, receivedMessage, currentWorker);

                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    if (currentWorker.IsDisposed == false)
                        currentWorker.PullingTask = null;
                }
            }
        }

        private async Task<bool> HandleBatchFromServerWorkers(List<SubscriptionServerWorker> workers, SubscriptionConnectionServerMessage receivedMessage, SubscriptionServerWorker currentWorker)
        {
            switch (receivedMessage.Type)
            {
                case SubscriptionConnectionServerMessage.MessageType.Data:
                case SubscriptionConnectionServerMessage.MessageType.Includes:
                case SubscriptionConnectionServerMessage.MessageType.CounterIncludes:
                case SubscriptionConnectionServerMessage.MessageType.TimeSeriesIncludes:
                    await WriteMessageToClientWorker(receivedMessage, currentWorker);
                    return true;
                case SubscriptionConnectionServerMessage.MessageType.EndOfBatch:
                    await WriteMessageToClientWorker(receivedMessage, currentWorker);
                    return false;
                default:
                    await HandleServerMessageFromServerWorkers(workers, receivedMessage, currentWorker);
                    return false;
            }
        }

        //TODO: egor handle double code with initasync
        private async Task HandleServerMessageFromServerWorkers(List<SubscriptionServerWorker> workers, SubscriptionConnectionServerMessage receivedMessage, SubscriptionServerWorker currentWorker)
        {
            switch (receivedMessage.Type)
            {
                case SubscriptionConnectionServerMessage.MessageType.Confirm:
                    break;
                case SubscriptionConnectionServerMessage.MessageType.ConnectionStatus:
                    switch (receivedMessage.Status)
                    {
                        case SubscriptionConnectionServerMessage.ConnectionStatus.None:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.InUse:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.ForbiddenReadOnly:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.Forbidden:
                        case SubscriptionConnectionServerMessage.ConnectionStatus.Invalid:
                            //TODO: egor throw on some of those statuses ?
                            CancellationTokenSource.Cancel();
                            break;
                        case SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect:
                            // drop subscription on other workers
                            foreach (var worker in workers)
                            {
                                if (worker != currentWorker)
                                {
                                    // propagate the error so the other server workers fail
                                    var t = SendClientReplyToServerWorker(worker, new SubscriptionConnectionClientMessage()
                                    {
                                        ChangeVector = _lastChangeVector,
                                        Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
                                    });
                                }
                            }

                            throw new SubscriptionChangeVectorUpdateConcurrencyException(receivedMessage.Message);
                        case SubscriptionConnectionServerMessage.ConnectionStatus.Redirect:
                            // try to connect to new worker
                            _workers.Remove(currentWorker);

                            SubscriptionServerWorker newWorker = null;
                            var database = currentWorker.Database;
                            var skippedNodes = new Dictionary<string, List<string>> { { database, new List<string> { currentWorker.NodeTag } } };
                            List<Exception> exceptions = new List<Exception>();
                            currentWorker.Dispose();
                            var re = TcpConnection.ShardedContext.RequestExecutors[ShardHelper.TryGetShardIndex(database)];

                            // TODO: egor do I need this?
                            if (re.Topology == null)
                            {
                                await re.WaitForTopologyUpdate(re._firstTopologyUpdate);
                                if (re.Topology == null)
                                {
                                    throw new Exception("ShouldNot happen and likely a  bug.!!");
                                }
                            }

                            var appropriateNode = receivedMessage.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();

                            var timeout = int.MaxValue;
                            var sp = Stopwatch.StartNew();
                            var sp2 = Stopwatch.StartNew();
                            while (skippedNodes.Count < re.Topology.Nodes.Count && sp.ElapsedMilliseconds < timeout)
                            {
                                while (skippedNodes.Count < re.Topology.Nodes.Count && sp.ElapsedMilliseconds < timeout)
                                {
                                    // try assert connection for each worker
                                    if (sp2.ElapsedMilliseconds >= 3000)
                                    {
                                        await SendHeartBeat("Waited for 3000ms for assert connection status to server workers");
                                        sp2.Reset();
                                    }

                                    if (string.IsNullOrEmpty(appropriateNode))
                                    {
                                        var selectedNode = re.Topology.Nodes.FirstOrDefault(x => skippedNodes[database].Contains(x.ClusterTag) == false);
                                        if (selectedNode == null)
                                        {
                                            _rejectedShards.Add(database);
                                            break;
                                        }

                                        try
                                        {
                                            newWorker = await GetShardedSubscriptionServerWorker(database, re, selectedNode.ClusterTag);
                                        }
                                        catch (Exception ex)
                                        {
                                            exceptions.Add(ex);
                                            skippedNodes[database].Add(selectedNode.ClusterTag);
                                        }

                                        if (newWorker != null)
                                        {
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        if (re.Topology.Nodes.Select(x => x.ClusterTag).Contains(appropriateNode) == false)
                                        {
                                            throw new InvalidOperationException(
                                                $"Shard '{database}' topology '{string.Join(", ", re.Topology.Nodes)}' doesn't contain the subscription's appropriate node '{appropriateNode}' should not happen and likely a bug.");
                                        }

                                        try
                                        {
                                            newWorker = await GetShardedSubscriptionServerWorker(database, re, appropriateNode);
                                        }
                                        catch (Exception e)
                                        {
                                            exceptions.Add(e);
                                            skippedNodes[database].Add(appropriateNode);
                                        }

                                        if (newWorker == null)
                                        {
                                            appropriateNode = null;
                                        }
                                        else
                                        {
                                            break;
                                        }
                                    }
                                }

                                if (newWorker == null)
                                {
                                    // we could not connect  to worker, have no nodes left to connect and could not create new worker => setup connect with timeout and continue to next worker.
                                    _rejectedShards.Add(database);
                                    break;
                                }

                                await SendOptionToServerWorker(newWorker);

                                // get connection status
                                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                                using (context.GetMemoryBuffer(out var buffer))
                                {
                                    var receivedMessage2 = await ReadNextObject(context, newWorker, buffer);
                                    if (receivedMessage2 == null || CancellationTokenSource.IsCancellationRequested)
                                        throw new SubscriptionClosedException($"Subscription '{SubscriptionId}' was closed");

                                    if (receivedMessage2.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
                                    {
                                        using (var bjro = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(receivedMessage2, context))
                                        {
                                            await WriteBlittableAsync(bjro, TcpConnection);
                                        }

                                        throw new Exception($"Server returned illegal type message when expecting connection status, was: {receivedMessage2.Type}");
                                    }

                                    if (receivedMessage2.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                                    {
                                        break;
                                    }
                                    else if (receivedMessage2.Status == SubscriptionConnectionServerMessage.ConnectionStatus.Redirect)
                                    {
                                        // add redirected node to skipped list
                                        skippedNodes[database].Add(newWorker.NodeTag);
                                        // check if shard returned appropriate node, and remove it from skipped nodes if needed
                                        appropriateNode = receivedMessage.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                                        if (string.IsNullOrEmpty(appropriateNode) == false)
                                            skippedNodes[database].Remove(appropriateNode);

                                        newWorker.Dispose();
                                    }
                                    else
                                    {
                                        throw new Exception($"TODO: egor, assert connection status failed in redirect in batch");
                                    }
                                }
                            }

                            if (sp.ElapsedMilliseconds > timeout)
                            {
                                newWorker?.Dispose();
                                newWorker = null;
                            }

                            if (newWorker == null)
                            {
                                //we could not connect  to worker, have no nodes left to connect and could not create new worker => setup connect with timeout and continue to next worker.
                                _rejectedShards.Add(database);
                                break;
                            }

                            currentWorker = newWorker;
                            _workers.Add(currentWorker);

                            break;
                    }

                    break;
                case SubscriptionConnectionServerMessage.MessageType.Error:
                    CancellationTokenSource.Cancel();
                    break;
                default:
                    throw new Exception($"Unexpected type {receivedMessage.Type}");
            }

            if (currentWorker.IsDisposed == false)
            {
                await WriteMessageToClientWorker(receivedMessage, currentWorker);
            }
        }

        private async Task WriteMessageToClientWorker(SubscriptionConnectionServerMessage receivedMessage, SubscriptionServerWorker currentWorker)
        {
            using (var bjro = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(receivedMessage, currentWorker.Context))
            {
                if (receivedMessage.Data != null)
                {
                    bjro.Modifications ??= new DynamicJsonValue(bjro);
                    bjro.Modifications[nameof(SubscriptionConnectionServerMessage.Data)] = receivedMessage.Data.Clone(currentWorker.Context);
                }
                if (receivedMessage.Includes != null)
                {
                    bjro.Modifications ??= new DynamicJsonValue(bjro);
                    bjro.Modifications[nameof(SubscriptionConnectionServerMessage.Includes)] = receivedMessage.Includes.Clone(currentWorker.Context);
                }
                if (receivedMessage.CounterIncludes != null)
                {
                    bjro.Modifications ??= new DynamicJsonValue(bjro);
                    bjro.Modifications[nameof(SubscriptionConnectionServerMessage.CounterIncludes)] = receivedMessage.CounterIncludes.Clone(currentWorker.Context);
                }
                if (receivedMessage.TimeSeriesIncludes != null)
                {
                    bjro.Modifications ??= new DynamicJsonValue(bjro);
                    bjro.Modifications[nameof(SubscriptionConnectionServerMessage.TimeSeriesIncludes)] = receivedMessage.TimeSeriesIncludes.Clone(currentWorker.Context);
                }

                if (bjro.Modifications == null)
                {
                    await WriteBlittableAsync(bjro, TcpConnection);
                    return;
                }

                using var bjroWithData = currentWorker.Context.ReadObject(bjro, "ShardedSubscription/ObjectWithData");
                await WriteBlittableAsync(bjroWithData, TcpConnection);
            }
        }

        private string _lastChangeVector;
        private async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAck(List<SubscriptionServerWorker> workers, Task<SubscriptionConnectionClientMessage> replyFromClientTask, SubscriptionServerWorker currentWorker)
        {
            SubscriptionConnectionClientMessage clientReply;
            while (true)
            {
                var result = await Task.WhenAny(replyFromClientTask, TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(5000), CancellationTokenSource.Token)).ConfigureAwait(false);
                CancellationTokenSource.Token.ThrowIfCancellationRequested();

                if (result == replyFromClientTask)
                {
                    clientReply = await replyFromClientTask;

                    if (clientReply.Type != SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
                        replyFromClientTask = SubscriptionConnection.GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);

                    break;
                }

                await SendHeartBeat("Waiting for client ACK");
            }

            if (clientReply.Type == SubscriptionConnectionClientMessage.MessageType.DisposedNotification)
            {
                foreach (var worker in workers)
                {
                    await SendClientReplyToServerWorker(worker, clientReply);
                }

                CancellationTokenSource.Cancel();
                return null;
            }

            _lastChangeVector = clientReply.ChangeVector;
            await SendClientReplyToServerWorker(currentWorker, clientReply);

            return replyFromClientTask;
        }

        private async Task SendClientReplyToServerWorker(SubscriptionServerWorker subscriptionServerWorker, SubscriptionConnectionClientMessage clientReply)
        {
            using (var optionsJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(clientReply, subscriptionServerWorker.Context))
            {
                await optionsJson.WriteJsonToAsync(subscriptionServerWorker.Stream, CancellationTokenSource.Token).ConfigureAwait(false);
                await subscriptionServerWorker.Stream.FlushAsync(CancellationTokenSource.Token).ConfigureAwait(false);
            }
        }

        private async Task<SubscriptionConnectionServerMessage> ReadNextObject(JsonOperationContext context, SubscriptionServerWorker worker, JsonOperationContext.MemoryBuffer buffer)
        {
            if (CancellationTokenSource.IsCancellationRequested || worker.TcpClient.Connected == false)
                return null;

            try
            {
                var blittable = await context.ParseToMemoryAsync(worker.Stream, "ShardedSubscription/nextObject", BlittableJsonDocumentBuilder.UsageMode.None, buffer).ConfigureAwait(false);
                blittable.BlittableValidation();
                return JsonDeserializationClient.SubscriptionNextObjectResult(blittable);
            }
            catch (ObjectDisposedException)
            {
                return null;
            }
        }

        private static async Task WriteBlittableAsync(BlittableJsonReaderObject value, TcpConnectionOptions tcpConnection)
        {
            int writtenBytes;
            using (tcpConnection.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, tcpConnection.Stream))
            {
                context.Write(writer, value);
                writtenBytes = writer.Position;
            }

            await tcpConnection.Stream.FlushAsync();
            tcpConnection.RegisterBytesSent(writtenBytes);
        }

        private async Task<SubscriptionServerWorker> GetShardedSubscriptionServerWorker(string shard, RequestExecutor re, string redirectNode = null)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var command = new GetTcpInfoForRemoteTaskCommand("Subscription/" + shard, shard, Options.SubscriptionName, verifyDatabase: true);
                if (redirectNode == null)
                {
                    await re.ExecuteAsync(command, context, token: CancellationTokenSource.Token);
                }
                else
                {
                    var server = re.Topology.Nodes.FirstOrDefault(x => x.ClusterTag == redirectNode);
                    if (server == null)
                    {
                        throw new Exception($"TODO: egor");
                    }

                    await re.ExecuteAsync(server, nodeIndex: null, context, command, token: CancellationTokenSource.Token);
                }

                var tcpInfo = command.Result;

                string chosenUrl;
                TcpClient tcpClient;
                var timeout = TcpConnection.ShardedContext.RequestExecutors.First().DefaultTimeout;
                (tcpClient, chosenUrl) = await TcpUtils.ConnectAsyncWithPriority(tcpInfo, timeout).ConfigureAwait(false);
                tcpClient.NoDelay = true;

                var stream = await TcpUtils.WrapStreamWithSslAsync(
                    tcpClient,
                    tcpInfo,
                    TcpConnection.ShardedContext.RequestExecutors.First().Certificate,
#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1)
                    null,
#endif
                    timeout).ConfigureAwait(false);

                var supportedFeatures = await TcpNegotiation.NegotiateProtocolVersionAsync(context, stream, new AsyncTcpNegotiateParameters
                {
                    Database = shard,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    Version = TcpConnectionHeaderMessage.SubscriptionTcpVersion,
                    ReadResponseAndGetVersionCallbackAsync = async (operationContext, writer, stream1, _) =>
                    {
                        using (var response = await operationContext.ReadForMemoryAsync(stream1, "Subscription/sharded/tcp-header-response").ConfigureAwait(false))
                        {
                            var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                            switch (reply.Status)
                            {
                                case TcpConnectionStatus.Ok:
                                    return reply.Version;
                                case TcpConnectionStatus.AuthorizationFailed:
                                    throw new AuthorizationException($"Cannot access shard '{shard}' because " + reply.Message);
                                case TcpConnectionStatus.TcpVersionMismatch:
                                    if (reply.Version != TcpNegotiation.OutOfRangeStatus)
                                        return reply.Version;

                                    //Kindly request the server to drop the connection
                                    operationContext.Write(writer, new DynamicJsonValue
                                    {
                                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = shard,
                                        [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop.ToString(),
                                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] =
                                            TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                                        [nameof(TcpConnectionHeaderMessage.Info)] =
                                            $"Couldn't agree on subscription TCP version ours: '{TcpConnectionHeaderMessage.SubscriptionTcpVersion}' theirs: '{reply.Version}'"
                                    });

                                    await writer.FlushAsync().ConfigureAwait(false);
                                    throw new InvalidOperationException($"Can't connect to shard '{shard}' because: {reply.Message}");
                            }

                            return reply.Version;
                        }
                    },
                    DestinationUrl = chosenUrl
                });

                if (supportedFeatures.ProtocolVersion <= 0)
                    throw new InvalidOperationException(
                        $"{Options.SubscriptionName}: TCP negotiation resulted with an invalid protocol version:{supportedFeatures.ProtocolVersion}");

                return new SubscriptionServerWorker(stream, tcpClient, _serverStore, shard, tcpInfo.NodeTag);
            }
        }

        private class SubscriptionServerWorker : IDisposable
        {
            private readonly ServerStore _serverStore;
            private IDisposable _returnContext;
            private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;
            public bool IsDisposed;

            public readonly Stream Stream;
            public readonly TcpClient TcpClient;
            public Task<SubscriptionConnectionServerMessage> PullingTask;
            public bool HasPullingTask => PullingTask != null;
            public JsonOperationContext Context;
            public JsonOperationContext.MemoryBuffer Buffer;
            public string Database;
            public string NodeTag;
            public SubscriptionServerWorker(Stream stream, TcpClient tcpClient, ServerStore serverStore, string database, string nodeTag)
            {
                Stream = stream;
                TcpClient = tcpClient;
                _serverStore = serverStore;
                Database = database;
                NodeTag = nodeTag;

                AllocateResources();
            }

            private void AllocateResources()
            {
                _returnContext = _serverStore.ContextPool.AllocateOperationContext(out Context);
                _returnBuffer = Context.GetMemoryBuffer(out Buffer);
            }

            public void Dispose()
            {
                if (IsDisposed)
                    return;

                IsDisposed = true;
                _returnBuffer.Dispose();
                _returnContext.Dispose();
                Stream.Dispose();
                TcpClient.Dispose();
            }
        }

        public new void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            base.Dispose();
            if (_workers != null)
            {
                foreach (SubscriptionServerWorker worker in _workers)
                {
                    worker.Dispose();
                }
            }
        }
    }
}
