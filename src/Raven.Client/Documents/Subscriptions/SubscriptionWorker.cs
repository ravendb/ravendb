// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionWorker<T> : IAsyncDisposable, IDisposable where T : class
    {
        public delegate Task AfterAcknowledgmentAction(SubscriptionBatch<T> batch);

        private readonly Logger _logger;
        private readonly DocumentStore _store;
        private readonly string _dbName;
        private CancellationTokenSource _processingCts = new CancellationTokenSource();
        private readonly SubscriptionWorkerOptions _options;
        private (Func<SubscriptionBatch<T>, Task> Async, Action<SubscriptionBatch<T>> Sync) _subscriber;
        internal TcpClient _tcpClient;
        private bool _disposed;
        private Task _subscriptionTask;
        private Stream _stream;
        private int _forcedTopologyUpdateAttempts = 0;

        /// <summary>
        /// allows the user to define stuff that happens after the confirm was received from the server (this way we know we won't
        /// get those documents again)
        /// </summary>
        public event AfterAcknowledgmentAction AfterAcknowledgment;

        public event Action<Exception> OnSubscriptionConnectionRetry;
        public event Action<Exception> OnUnexpectedSubscriptionError;

        internal SubscriptionWorker(SubscriptionWorkerOptions options, DocumentStore documentStore, string dbName)
        {
            _options = options;
            if (string.IsNullOrEmpty(options.SubscriptionName))
                throw new ArgumentException("SubscriptionConnectionOptions must specify the SubscriptionName", nameof(options));
            _store = documentStore;
            _dbName = _store.GetDatabase(dbName);
            _logger = LoggingSource.Instance.GetLogger<SubscriptionWorker<T>>(_dbName);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool waitForSubscriptionTask)
        {
            if (_disposed)
                return;

            var dispose = DisposeAsync(waitForSubscriptionTask);
            if (dispose.IsCompletedSuccessfully)
                return;

            AsyncHelpers.RunSync(() => dispose.AsTask());
        }

        public ValueTask DisposeAsync()
        {
            return DisposeAsync(true);
        }

        public async ValueTask DisposeAsync(bool waitForSubscriptionTask)
        {
            try
            {
                if (_disposed)
                    return;

                _disposed = true;
                _processingCts?.Cancel();

                CloseTcpClient(); // we disconnect immediately, freeing the subscription task

                if (_subscriptionTask != null && waitForSubscriptionTask)
                {
                    try
                    {
                        await _subscriptionTask.ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // just need to wait for it to end
                    }
                }

                _subscriptionLocalRequestExecutor?.Dispose();
                _processingCts?.Dispose();
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error during dispose of subscription", ex);
            }
            finally
            {
                OnDisposed(this);
            }
        }

        public Task Run(Action<SubscriptionBatch<T>> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (null, processDocuments);
            return Run(ct);
        }

        public Task Run(Func<SubscriptionBatch<T>, Task> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (processDocuments, null);
            return Run(ct);
        }

        private Task Run(CancellationToken ct)
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException("The subscription is already running");

            if (ct != default)
            {
                _processingCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            }

            return _subscriptionTask = RunSubscriptionAsync();
        }

        private ServerNode _redirectNode;
        private RequestExecutor _subscriptionLocalRequestExecutor;

        public string CurrentNodeTag => _redirectNode?.ClusterTag;
        public string SubscriptionName => _options?.SubscriptionName;

        internal int? SubscriptionTcpVersion;

        private async Task<Stream> ConnectToServer(CancellationToken token)
        {
            var command = new GetTcpInfoForRemoteTaskCommand("Subscription/" + _dbName, _dbName, _options?.SubscriptionName, verifyDatabase: true);

            var requestExecutor = _store.GetRequestExecutor(_dbName);

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                TcpConnectionInfo tcpInfo;
                if (_redirectNode != null)
                {
                    try
                    {
                        await requestExecutor.ExecuteAsync(_redirectNode, null, context, command, shouldRetry: false, sessionInfo: null, token: token)
                            .ConfigureAwait(false);
                        tcpInfo = command.Result;
                    }
                    catch (ClientVersionMismatchException)
                    {
                        tcpInfo = await LegacyTryGetTcpInfo(requestExecutor, context, _redirectNode, token).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // if we failed to talk to a node, we'll forget about it and let the topology to
                        // redirect us to the current node
                        _redirectNode = null;
                        throw;
                    }
                }
                else
                {
                    try
                    {
                        await requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                        tcpInfo = command.Result;
                        if (tcpInfo.NodeTag != null)
                        {
                            _redirectNode = requestExecutor.Topology.Nodes.FirstOrDefault(x => x.ClusterTag == tcpInfo.NodeTag);
                        }
                    }
                    catch (ClientVersionMismatchException)
                    {
                        tcpInfo = await LegacyTryGetTcpInfo(requestExecutor, context, token).ConfigureAwait(false);
                    }
                }

                string chosenUrl;
                (_tcpClient, chosenUrl) = await TcpUtils.ConnectAsyncWithPriority(tcpInfo, requestExecutor.DefaultTimeout).ConfigureAwait(false);
                _tcpClient.NoDelay = true;
                _tcpClient.SendBufferSize = _options?.SendBufferSizeInBytes ?? SubscriptionWorkerOptions.DefaultSendBufferSizeInBytes;
                _tcpClient.ReceiveBufferSize = _options?.ReceiveBufferSizeInBytes ?? SubscriptionWorkerOptions.DefaultReceiveBufferSizeInBytes;
                _stream = _tcpClient.GetStream();
                _stream = await TcpUtils.WrapStreamWithSslAsync(
                    _tcpClient,
                    tcpInfo,
                    _store.Certificate,
#if !(NETSTANDARD2_0 || NETSTANDARD2_1 || NETCOREAPP2_1)
                    null,
#endif
                    requestExecutor.DefaultTimeout).ConfigureAwait(false);

                var databaseName = _store.GetDatabase(_dbName);

                var parameters = new TcpNegotiateParameters
                {
                    Database = databaseName,
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    Version = SubscriptionTcpVersion ?? TcpConnectionHeaderMessage.SubscriptionTcpVersion,
                    ReadResponseAndGetVersionCallback = ReadServerResponseAndGetVersion,
                    DestinationNodeTag = CurrentNodeTag,
                    DestinationUrl = chosenUrl
                };
                _supportedFeatures = TcpNegotiation.NegotiateProtocolVersion(context, _stream, parameters);

                if (_supportedFeatures.ProtocolVersion <= 0)
                {
                    throw new InvalidOperationException(
                        $"{_options.SubscriptionName}: TCP negotiation resulted with an invalid protocol version:{_supportedFeatures.ProtocolVersion}");
                }

                using (var optionsJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, context))
                {
                    optionsJson.WriteJsonTo(_stream);

                    await _stream.FlushAsync(token).ConfigureAwait(false);
                }

                _subscriptionLocalRequestExecutor?.Dispose();
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(command.RequestedNode.Url, _dbName, requestExecutor.Certificate, _store.Conventions);
                _store.RegisterEvents(_subscriptionLocalRequestExecutor);

                return _stream;
            }
        }

        private async Task<TcpConnectionInfo> LegacyTryGetTcpInfo(RequestExecutor requestExecutor, JsonOperationContext context, CancellationToken token)
        {
            var tcpCommand = new GetTcpInfoCommand("Subscription/" + _dbName, _dbName);
            try
            {
                await requestExecutor.ExecuteAsync(tcpCommand, context, sessionInfo: null, token: token)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                _redirectNode = null;
                throw;
            }

            return tcpCommand.Result;
        }

        private async Task<TcpConnectionInfo> LegacyTryGetTcpInfo(RequestExecutor requestExecutor, JsonOperationContext context, ServerNode node, CancellationToken token)
        {
            var tcpCommand = new GetTcpInfoCommand("Subscription/" + _dbName, _dbName);
            try
            {
                await requestExecutor.ExecuteAsync(node, null, context, tcpCommand, shouldRetry: false, sessionInfo: null, token: token)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                _redirectNode = null;
                throw;
            }

            return tcpCommand.Result;
        }

        private int ReadServerResponseAndGetVersion(JsonOperationContext context, BlittableJsonTextWriter writer, Stream stream, string url)
        {
            //Reading reply from server
            using (var response = context.ReadForMemory(_stream, "Subscription/tcp-header-response"))
            {
                var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                switch (reply.Status)
                {
                    case TcpConnectionStatus.Ok:
                        return reply.Version;
                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new AuthorizationException($"Cannot access database {_dbName} because " + reply.Message);
                    case TcpConnectionStatus.TcpVersionMismatch:
                        if (reply.Version != TcpNegotiation.OutOfRangeStatus)
                        {
                            return reply.Version;
                        }
                        //Kindly request the server to drop the connection
                        SendDropMessage(context, writer, reply);
                        throw new InvalidOperationException($"Can't connect to database {_dbName} because: {reply.Message}");
                }
                return reply.Version;
            }
        }

        private void SendDropMessage(JsonOperationContext context, BlittableJsonTextWriter writer, TcpConnectionHeaderResponse reply)
        {
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _dbName,
                [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop.ToString(),
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                [nameof(TcpConnectionHeaderMessage.Info)] =
                    $"Couldn't agree on subscription TCP version ours:{TcpConnectionHeaderMessage.SubscriptionTcpVersion} theirs:{reply.Version}"
            });
            writer.Flush();
        }

        private void AssertConnectionState(SubscriptionConnectionServerMessage connectionStatus)
        {
            if (connectionStatus.Type == SubscriptionConnectionServerMessage.MessageType.Error)
            {
                if (connectionStatus.Exception.Contains(nameof(DatabaseDoesNotExistException)))
                    DatabaseDoesNotExistException.ThrowWithMessage(_dbName, connectionStatus.Message);
            }
            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
                throw new Exception("Server returned illegal type message when expecting connection status, was: " +
                                    connectionStatus.Type);

            switch (connectionStatus.Status)
            {
                case SubscriptionConnectionServerMessage.ConnectionStatus.Accepted:
                    break;
                case SubscriptionConnectionServerMessage.ConnectionStatus.InUse:
                    throw new SubscriptionInUseException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it's in use and the connection strategy is {_options.Strategy}");
                case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                    bool canReconnect = false;
                    connectionStatus.Data?.TryGet(nameof(SubscriptionClosedException.CanReconnect), out canReconnect);
                    throw new SubscriptionClosedException($"Subscription With Id '{_options.SubscriptionName}' was closed.  " + connectionStatus.Exception, canReconnect);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Invalid:
                    throw new SubscriptionInvalidStateException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it is in invalid state. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it does not exist. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Redirect:
                    var appropriateNode = connectionStatus.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                    var rawReasons = connectionStatus.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.Reasons)];
                    Dictionary<string, string> reasonsDictionary = new Dictionary<string, string>();
                    if (rawReasons != null && rawReasons is BlittableJsonReaderArray rawReasonsArray)
                    {
                        foreach (var item in rawReasonsArray)
                        {
                            if (item is BlittableJsonReaderObject itemAsBlittable)
                            {
                                Debug.Assert(itemAsBlittable.Count == 1);
                                if (itemAsBlittable.Count == 1)
                                {
                                    var tagName = itemAsBlittable.GetPropertyNames()[0];
                                    reasonsDictionary[tagName] = itemAsBlittable[tagName].ToString();
                                }
                            }
                        }
                    }
                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be processed by current node, it will be redirected to {appropriateNode}]{Environment.NewLine}Reasons:{string.Join(Environment.NewLine, reasonsDictionary.Select(x => $"{x.Key}:{x.Value}"))}",
                        inner: new SubscriptionDoesNotBelongToNodeException(connectionStatus.Exception),
                        appropriateNode,
                        reasonsDictionary);
                case SubscriptionConnectionServerMessage.ConnectionStatus.ConcurrencyReconnect:
                    throw new SubscriptionChangeVectorUpdateConcurrencyException(connectionStatus.Message);
                default:
                    throw new ArgumentException(
                        $"Subscription '{_options.SubscriptionName}' could not be opened, reason: {connectionStatus.Status}");
            }
        }

        private async Task ProcessSubscriptionAsync()
        {
            try
            {
                _processingCts.Token.ThrowIfCancellationRequested();

                var contextPool = _store.GetRequestExecutor(_dbName).ContextPool;

                using (contextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                using (var tcpStream = await ConnectToServer(_processingCts.Token).ConfigureAwait(false))
                {
                    _processingCts.Token.ThrowIfCancellationRequested();
                    var tcpStreamCopy = tcpStream;
                    using (contextPool.AllocateOperationContext(out JsonOperationContext handshakeContext))
                    {
                        var connectionStatus = await ReadNextObject(handshakeContext, tcpStreamCopy, buffer).ConfigureAwait(false);
                        if (_processingCts.IsCancellationRequested)
                            return;

                        if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus ||
                            connectionStatus.Status != SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                            AssertConnectionState(connectionStatus);
                    }
                    _lastConnectionFailure = null;
                    if (_processingCts.IsCancellationRequested)
                        return;

                    Task notifiedSubscriber = Task.CompletedTask;

                    var batch = new SubscriptionBatch<T>(_subscriptionLocalRequestExecutor, _store, _dbName, _logger);

                    while (_processingCts.IsCancellationRequested == false)
                    {
                        // start the read from the server
                        var readFromServer = ReadSingleSubscriptionBatchFromServer(contextPool, tcpStreamCopy, buffer, batch);
                        try
                        {
                            // and then wait for the subscriber to complete
                            await notifiedSubscriber.ConfigureAwait(false);
                        }
                        catch (Exception)
                        {
                            // if the subscriber errored, we shut down
                            try
                            {
                                CloseTcpClient();
                                using ((await readFromServer.ConfigureAwait(false)).ReturnContext)
                                {
                                }
                            }
                            catch (Exception)
                            {
                                // nothing to be done here
                            }
                            throw;
                        }
                        var incomingBatch = await readFromServer.ConfigureAwait(false);

                        _processingCts.Token.ThrowIfCancellationRequested();

                        var lastReceivedChangeVector = batch.Initialize(incomingBatch);

                        notifiedSubscriber = Task.Run(async () =>
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            using (incomingBatch.ReturnContext)
                            {
                                try
                                {
                                    if (_subscriber.Async != null)
                                        await _subscriber.Async(batch).ConfigureAwait(false);
                                    else
                                        _subscriber.Sync(batch);
                                }
                                catch (Exception ex)
                                {
                                    if (_logger.IsInfoEnabled)
                                    {
                                        _logger.Info(
                                            $"Subscription '{_options.SubscriptionName}'. Subscriber threw an exception on document batch", ex);
                                    }

                                    if (_options.IgnoreSubscriberErrors == false)
                                        throw new SubscriberErrorException($"Subscriber threw an exception in subscription '{_options.SubscriptionName}'", ex);
                                }
                            }

                            try
                            {
                                if (tcpStreamCopy != null) //possibly prevent ObjectDisposedException
                                {
                                    await SendAckAsync(lastReceivedChangeVector, tcpStreamCopy, context).ConfigureAwait(false);
                                }
                            }
                            catch (ObjectDisposedException)
                            {
                                //if this happens, this means we are disposing, so don't care..
                                //(this piece of code happens asynchronously to external using(tcpStream) statement)
                            }
                        });
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (_disposed == false)
                    throw;

                // otherwise this is thrown when shutting down, it
                // isn't an error, so we don't need to treat
                // it as such
            }
        }

        private async Task<BatchFromServer> ReadSingleSubscriptionBatchFromServer(JsonContextPool contextPool, Stream tcpStream, JsonOperationContext.MemoryBuffer buffer, SubscriptionBatch<T> batch)
        {
            var incomingBatch = new List<SubscriptionConnectionServerMessage>();
            var includes = new List<BlittableJsonReaderObject>();
            var counterIncludes = new List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)>();
            var timeSeriesIncludes = new List<BlittableJsonReaderObject>();
            IDisposable returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);
            bool endOfBatch = false;
            while (endOfBatch == false && _processingCts.IsCancellationRequested == false)
            {
                SubscriptionConnectionServerMessage receivedMessage = await ReadNextObject(context, tcpStream, buffer).ConfigureAwait(false);
                if (receivedMessage == null || _processingCts.IsCancellationRequested)
                {
                    break;
                }

                switch (receivedMessage.Type)
                {
                    case SubscriptionConnectionServerMessage.MessageType.Data:
                        incomingBatch.Add(receivedMessage);
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.Includes:
                        includes.Add(receivedMessage.Includes);
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.CounterIncludes:
                        counterIncludes.Add((receivedMessage.CounterIncludes, receivedMessage.IncludedCounterNames));
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.TimeSeriesIncludes:
                        timeSeriesIncludes.Add(receivedMessage.TimeSeriesIncludes);
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.EndOfBatch:
                        endOfBatch = true;
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.Confirm:
                        var onAfterAcknowledgment = AfterAcknowledgment;
                        if (onAfterAcknowledgment != null)
                            await onAfterAcknowledgment(batch).ConfigureAwait(false);
                        incomingBatch.Clear();
                        batch.Items.Clear();
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.ConnectionStatus:
                        AssertConnectionState(receivedMessage);
                        break;
                    case SubscriptionConnectionServerMessage.MessageType.Error:
                        ThrowSubscriptionError(receivedMessage);
                        break;
                    default:
                        ThrowInvalidServerResponse(receivedMessage);
                        break;
                }
            }
            return new BatchFromServer
            {
                Messages = incomingBatch,
                ReturnContext = returnContext,
                Includes = includes,
                CounterIncludes = counterIncludes,
                TimeSeriesIncludes = timeSeriesIncludes
            };
        }

        private static void ThrowInvalidServerResponse(SubscriptionConnectionServerMessage receivedMessage)
        {
            throw new ArgumentException(
                $"Unrecognized message '{receivedMessage.Type}' type received from server");
        }

        private static void ThrowSubscriptionError(SubscriptionConnectionServerMessage receivedMessage)
        {
            throw new InvalidOperationException(
                $"Connection terminated by server. Exception: {receivedMessage.Exception ?? "None"}");
        }

        private async Task<SubscriptionConnectionServerMessage> ReadNextObject(JsonOperationContext context, Stream stream, JsonOperationContext.MemoryBuffer buffer)
        {
            if (_processingCts.IsCancellationRequested || _tcpClient.Connected == false)
                return null;

            if (_disposed) //if we are disposed, nothing to do...
                return null;

            try
            {
                var blittable = await context.ParseToMemoryAsync(stream, "Subscription/next/object", BlittableJsonDocumentBuilder.UsageMode.None, buffer)
                    .ConfigureAwait(false);

                blittable.BlittableValidation();
                return JsonDeserializationClient.SubscriptionNextObjectResult(blittable);
            }
            catch (ObjectDisposedException)
            {
                //this can happen only if Subscription<T> is disposed, and in this case we don't care about a result...
                return null;
            }
        }

        private async Task SendAckAsync(string lastReceivedChangeVector, Stream stream, JsonOperationContext context)
        {
            var message = new SubscriptionConnectionClientMessage
            {
                ChangeVector = lastReceivedChangeVector,
                Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
            };

            using (var messageJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(message, context))
            {
                messageJson.WriteJsonTo(stream);

                await stream.FlushAsync().ConfigureAwait(false);
            }
        }

        private async Task RunSubscriptionAsync()
        {
            while (_processingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    if (_forTestingPurposes != null && _forTestingPurposes.SimulateUnexpectedException)
                        throw new InvalidOperationException("SimulateUnexpectedException");

                    CloseTcpClient();
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Subscription '{_options.SubscriptionName}'. Connecting to server...");
                    }

                    await ProcessSubscriptionAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    while (_recentExceptions.Count >= 10)
                        _recentExceptions.TryDequeue(out _);

                    _recentExceptions.Enqueue(ex);
                    try
                    {
                        if (_processingCts.Token.IsCancellationRequested)
                        {
                            if (_disposed == false)
                                throw;
                            return;
                        }

                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info(
                                $"Subscription '{_options.SubscriptionName}'. Pulling task threw the following exception", ex);
                        }
                        if (ShouldTryToReconnect(ex))
                        {
                            await TimeoutManager.WaitFor(_options.TimeToWaitBeforeConnectionRetry).ConfigureAwait(false);

                            if (_redirectNode == null)
                            {
                                var reqEx = _store.GetRequestExecutor(_dbName);
                                var curTopology = reqEx.TopologyNodes;
                                var nextNodeIndex = (_forcedTopologyUpdateAttempts++) % curTopology.Count;
                                _redirectNode = curTopology[nextNodeIndex];
                                if (_logger.IsInfoEnabled)
                                {
                                    _logger.Info(
                                        $"Subscription '{_options.SubscriptionName}'. Will modify redirect node from null to {_redirectNode.ClusterTag}", ex);
                                }
                            }

                            var onSubscriptionConnectionRetry = OnSubscriptionConnectionRetry;
                            onSubscriptionConnectionRetry?.Invoke(ex);
                        }
                        else
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Connection to subscription '{_options.SubscriptionName}' have been shut down because of an error", ex);

                            throw;
                        }
                    }
                    catch (Exception e)
                    {
                        if (e == ex)
                            throw;

                        throw new AggregateException(new[] { e }.Concat(_recentExceptions));
                    }
                }
            }
        }

        private DateTime? _lastConnectionFailure;
        private TcpConnectionHeaderMessage.SupportedFeatures _supportedFeatures;
        private ConcurrentQueue<Exception> _recentExceptions = new ConcurrentQueue<Exception>();

        private void AssertLastConnectionFailure()
        {
            if (_lastConnectionFailure == null)
            {
                _lastConnectionFailure = DateTime.Now;
                return;
            }

            if ((DateTime.Now - _lastConnectionFailure) > _options.MaxErroneousPeriod)
            {
                throw new SubscriptionInvalidStateException(
                    $"Subscription connection was in invalid state for more than {_options.MaxErroneousPeriod} and therefore will be terminated");
            }
        }

        private bool ShouldTryToReconnect(Exception ex)
        {
            switch (ex)
            {
                case SubscriptionDoesNotBelongToNodeException se:
                    var requestExecutor = _store.GetRequestExecutor(_dbName);

                    if (se.AppropriateNode == null)
                    {
                        AssertLastConnectionFailure();
                        _redirectNode = null;
                        return true;
                    }

                    var nodeToRedirectTo = requestExecutor.TopologyNodes
                        .FirstOrDefault(x => x.ClusterTag == se.AppropriateNode);
                    _redirectNode = nodeToRedirectTo ?? throw new AggregateException(ex,
                                        new InvalidOperationException($"Could not redirect to {se.AppropriateNode}, because it was not found in local topology, even after retrying"));

                    return true;
                case NodeIsPassiveException e:
                    {
                        // if we failed to talk to a node, we'll forget about it and let the topology to
                        // redirect us to the current node
                        _redirectNode = null;
                        return true;
                    }
                case SubscriptionChangeVectorUpdateConcurrencyException _:
                    return true;
                case SubscriptionClosedException sce:
                    if (sce.CanReconnect)
                        return true;

                    _processingCts.Cancel();
                    return false;
                case SubscriptionInUseException _:
                case SubscriptionDoesNotExistException _:
                case SubscriptionInvalidStateException _:
                case DatabaseDoesNotExistException _:
                case AuthorizationException _:
                case AllTopologyNodesDownException _:
                case SubscriberErrorException _:
                case RavenException _:
                    _processingCts.Cancel();
                    return false;
                default:
                    OnUnexpectedSubscriptionError?.Invoke(ex);
                    AssertLastConnectionFailure();
                    return true;
            }
        }

        private void CloseTcpClient()
        {
            if (_stream != null)
            {
                try
                {
                    _stream.Dispose();
                    _stream = null;
                }
                catch (Exception)
                {
                    // can't do anything here
                }
            }
            if (_tcpClient != null)
            {
                try
                {
                    _tcpClient.Dispose();
                    _tcpClient = null;
                }
                catch (Exception)
                {
                    // nothing to be done
                }
            }
        }

        public event Action<SubscriptionWorker<T>> OnDisposed = delegate { };

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal bool SimulateUnexpectedException;
        }
    }
}
