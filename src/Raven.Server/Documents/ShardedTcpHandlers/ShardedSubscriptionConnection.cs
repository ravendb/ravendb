// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
// ReSharper disable AccessToDisposedClosure

namespace Raven.Server.Documents.ShardedTcpHandlers
{
    public class ShardedSubscriptionConnection : SubscriptionConnectionBase, IDisposable
    {
        private List<SubscriptionServerWorker> _workers;
        private bool _isDisposed;

        private ShardedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnectionOptions, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnectionOptions, serverStore, buffer, tcpConnectionDisposable)
        {
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
                                     await ReportExceptionToClient(server, tcpConnectionOptions, connection: null, e, connection._logger);
                                }
                                catch (Exception)
                                {
                                    // ignored
                                }
                            }
                            finally
                            {
                                connection.AddToStatusDescription("Finished processing subscription");
                                if (connection._logger.IsInfoEnabled)
                                {
                                    connection._logger.Info($"Finished processing sharded subscription {connection.SubscriptionId} / from client {remoteEndPoint}");
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
                (var options, long? id) = await ParseSubscriptionOptionsAsync(context, _serverStore, TcpConnection, _copiedBuffer.Buffer, TcpConnection.ShardedContext.GetShardedDatabaseName(), CancellationTokenSource.Token);
                _options = options;
                if (id.HasValue)
                    SubscriptionId = id.Value;
            }

            SubscriptionState = await TcpConnection.ShardedContext.ShardedSubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);

            _workers = new List<SubscriptionServerWorker>();
            var timeout = TcpConnection.ShardedContext.RequestExecutors.First().DefaultTimeout;
            for (int i = 0; i < TcpConnection.ShardedContext.Count; i++)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var db = TcpConnection.ShardedContext.GetShardedDatabaseName(i);
                    //TODO: egor check the endpoint with deleted database
                    var command = new GetTcpInfoForRemoteTaskCommand("Subscription/" + db, db, Options.SubscriptionName, verifyDatabase: true);
                    await TcpConnection.ShardedContext.RequestExecutors[i].ExecuteAsync(command, context);
                    var tcpInfo = command.Result;

                    string chosenUrl;
                    TcpClient tcpClient;
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
                        Database = db,
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
                                        throw new AuthorizationException($"Cannot access shard '{db}' because " + reply.Message);
                                    case TcpConnectionStatus.TcpVersionMismatch:
                                        if (reply.Version != TcpNegotiation.OutOfRangeStatus)
                                            return reply.Version;

                                        //Kindly request the server to drop the connection
                                        operationContext.Write(writer, new DynamicJsonValue
                                        {
                                            [nameof(TcpConnectionHeaderMessage.DatabaseName)] = db,
                                            [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop.ToString(),
                                            [nameof(TcpConnectionHeaderMessage.OperationVersion)] =
                                        TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                                            [nameof(TcpConnectionHeaderMessage.Info)] = $"Couldn't agree on subscription TCP version ours: '{TcpConnectionHeaderMessage.SubscriptionTcpVersion}' theirs: '{reply.Version}'"
                                        });

                                        await writer.FlushAsync().ConfigureAwait(false);
                                        throw new InvalidOperationException($"Can't connect to shard '{db}' because: {reply.Message}");
                                }

                                return reply.Version;
                            }
                        },
                        DestinationUrl = chosenUrl
                    });

                    if (supportedFeatures.ProtocolVersion <= 0)
                        throw new InvalidOperationException($"{Options.SubscriptionName}: TCP negotiation resulted with an invalid protocol version:{supportedFeatures.ProtocolVersion}");


                    _workers.Add(new SubscriptionServerWorker(stream, tcpClient, _serverStore));
                }
            }

            await TryConnectSubscription();
            _pendingConnectionScope.Dispose();

            try
            {
                _activeConnectionScope = _connectionScope.For(SubscriptionOperationScope.ConnectionActive);

                // refresh subscription data (change vector may have been updated, because in the meanwhile, another subscription could have just completed a batch)
                SubscriptionState = await TcpConnection.ShardedContext.ShardedSubscriptionStorage.AssertSubscriptionConnectionDetails(SubscriptionId, _options.SubscriptionName);
                foreach (var worker in _workers)
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
            }
            catch
            {
                DisposeOnDisconnect.Dispose();
                throw;
            }

            _connectionState = TcpConnection.ShardedContext.ShardedSubscriptionStorage.OpenSubscription(this);
            _connectionState.PendingConnections.Add(this);

            // Assert connection status
            foreach (var worker in _workers)
            {
                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                {
                    var receivedMessage = await ReadNextObject(context, worker, buffer);
                    if (receivedMessage == null || CancellationTokenSource.IsCancellationRequested)
                        return;

                    if (receivedMessage.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus ||
                        receivedMessage.Status != SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                    {
                        using (var bjro = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(receivedMessage, context))
                        {
                            await WriteBlittableAsync(bjro, TcpConnection);
                        }
                    }
                }
            }

            await WriteJsonAsync(new DynamicJsonValue
            {
                [nameof(SubscriptionConnectionServerMessage.Type)] = nameof(SubscriptionConnectionServerMessage.MessageType.ConnectionStatus),
                [nameof(SubscriptionConnectionServerMessage.Status)] = nameof(SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
            }, TcpConnection);
        }

        private async Task ProcessSubscriptionAsync()
        {
            var replyFromClientTask = GetReplyFromClientAsync(TcpConnection, _copiedBuffer.Buffer, _isDisposed);
            using (DisposeOnDisconnect)
            {

                while (CancellationTokenSource.IsCancellationRequested == false)
                {
                    foreach (SubscriptionServerWorker worker in _workers)
                    {
                        if (CancellationTokenSource.IsCancellationRequested)
                            break;

                        if (worker.HasPullingTask)
                            continue;

                        worker.PullingTask = ReadNextObject(worker.Context, worker, worker.Buffer);
                    }

                    var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromMilliseconds(3000));
                    var completedTask = await Task.WhenAny(new[] { replyFromClientTask, timeoutTask }.Concat(_workers.Select(x => x.PullingTask).ToList()));

                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    if (completedTask == replyFromClientTask)
                    {
                        // forward to shards
                        var clientReply = await replyFromClientTask;
                        foreach (var worker in _workers)
                        {
                            await SendClientReplyToWorker(worker, clientReply);
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

                    var currentWorker = _workers.First(x => x.PullingTask == completedTask);
                    var receivedMessage = await currentWorker.PullingTask;

                    var hasMore = true;
                    while (CancellationTokenSource.IsCancellationRequested == false && hasMore)
                    {
                        hasMore = await HandleBatchFromServerWorkers(receivedMessage, currentWorker);

                        currentWorker.PullingTask = ReadNextObject(currentWorker.Context, currentWorker, currentWorker.Buffer);
                        if (hasMore)
                        {
                            receivedMessage = await currentWorker.PullingTask;  // TODO: egor add timeout?
                        }
                    }

                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    // finished batch lets wait for client worker ack
                    replyFromClientTask = await WaitForClientAck(replyFromClientTask, currentWorker);
                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    // finished batch lets wait for server worker confirm
                    receivedMessage = await currentWorker.PullingTask;  // TODO: egor add timeout?

                    await HandleServerMessageFromServerWorkers(receivedMessage, currentWorker);

                    if (CancellationTokenSource.IsCancellationRequested)
                        break;

                    currentWorker.PullingTask = ReadNextObject(currentWorker.Context, currentWorker, currentWorker.Buffer);
                }
            }
        }

        private async Task<bool> HandleBatchFromServerWorkers(SubscriptionConnectionServerMessage receivedMessage, SubscriptionServerWorker currentWorker)
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
                    await HandleServerMessageFromServerWorkers(receivedMessage, currentWorker);
                    return false;
            }
        }

        private async Task HandleServerMessageFromServerWorkers(SubscriptionConnectionServerMessage receivedMessage, SubscriptionServerWorker currentWorker)
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
                            foreach (var worker in _workers)
                            {
                                if (worker != currentWorker)
                                {
                                    // propagate the error so the other server workers fail
                                    var t = SendClientReplyToWorker(worker, new SubscriptionConnectionClientMessage()
                                    {
                                        ChangeVector = _lastChangeVector,
                                        Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
                                    });
                                }
                            }

                            throw new SubscriptionChangeVectorUpdateConcurrencyException(receivedMessage.Message);
                    }

                    break;
                case SubscriptionConnectionServerMessage.MessageType.Error:
                    CancellationTokenSource.Cancel();
                    break;
                default:
                    throw new Exception($"Unexpected type {receivedMessage.Type}");
            }

            await WriteMessageToClientWorker(receivedMessage, currentWorker);
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
        private async Task<Task<SubscriptionConnectionClientMessage>> WaitForClientAck(Task<SubscriptionConnectionClientMessage> replyFromClientTask, SubscriptionServerWorker currentWorker)
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
                foreach (var worker in _workers)
                {
                    await SendClientReplyToWorker(worker, clientReply);
                }

                CancellationTokenSource.Cancel();
                return null;
            }

            _lastChangeVector = clientReply.ChangeVector;
            await SendClientReplyToWorker(currentWorker, clientReply);

            return replyFromClientTask;
        }

        private async Task SendClientReplyToWorker(SubscriptionServerWorker subscriptionServerWorker, SubscriptionConnectionClientMessage clientReply)
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

        private class SubscriptionServerWorker : IDisposable
        {
            private readonly ServerStore _serverStore;
            private IDisposable _returnContext;
            private JsonOperationContext.MemoryBuffer.ReturnBuffer _returnBuffer;

            public readonly Stream Stream;
            public readonly TcpClient TcpClient;
            public Task<SubscriptionConnectionServerMessage> PullingTask;
            public bool HasPullingTask => PullingTask != null;
            public JsonOperationContext Context;
            public JsonOperationContext.MemoryBuffer Buffer;

            public SubscriptionServerWorker(Stream stream, TcpClient tcpClient, ServerStore serverStore)
            {
                Stream = stream;
                TcpClient = tcpClient;
                _serverStore = serverStore;

                AllocateResources();
            }

            private void AllocateResources()
            {
                _returnContext = _serverStore.ContextPool.AllocateOperationContext(out Context);
                _returnBuffer = Context.GetMemoryBuffer(out Buffer);
            }

            public void Dispose()
            {
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
