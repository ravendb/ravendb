// -----------------------------------------------------------------------
//  <copyright file="SubscriptionWorker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Exceptions.Sharding;
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
    public abstract class AbstractSubscriptionWorker<TBatch, TType> : IAsyncDisposable, IDisposable
        where TBatch : SubscriptionBatchBase<TType>
        where TType : class
    {
        public delegate Task AfterAcknowledgmentAction(TBatch batch);
        protected readonly IRavenLogger _logger;
        internal readonly string _dbName;
        protected CancellationTokenSource _processingCts = new CancellationTokenSource();
        protected readonly SubscriptionWorkerOptions _options;
        protected (Func<TBatch, Task> Async, Action<TBatch> Sync) _subscriber;
        internal TcpClient _tcpClient;
        protected bool _disposed;
        protected Task _subscriptionTask;
        protected Stream _stream;
        protected int _forcedTopologyUpdateAttempts = 0;

        public string WorkerId => _options.WorkerId;
        /// <summary>
        /// Allows the user to define stuff that happens after the confirm was received from the server
        /// (this way we know we won't get those documents again)
        /// </summary>
        public event AfterAcknowledgmentAction AfterAcknowledgment;

        internal event Action OnEstablishedSubscriptionConnection;

        public event Action<Exception> OnSubscriptionConnectionRetry;

        public event Action<Exception> OnUnexpectedSubscriptionError;

        internal AbstractSubscriptionWorker(SubscriptionWorkerOptions options, string dbName, IRavenLogger logger)
        {
            if (string.IsNullOrEmpty(options.SubscriptionName))
                throw new ArgumentException("SubscriptionConnectionOptions must specify the SubscriptionName", nameof(options));

            _options = options;
            _dbName = dbName;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool waitForSubscriptionTask)
        {
            var dispose = DisposeAsync(waitForSubscriptionTask).AsTask();
            AsyncHelpers.RunSync(() => dispose);
        }

        public ValueTask DisposeAsync() => DisposeAsync(true);

        public virtual async ValueTask DisposeAsync(bool waitForSubscriptionTask)
        {
            if (_disposed)
                return;

            try
            {
                _disposed = true;
                _processingCts?.Cancel();

                CloseTcpClient(); // we disconnect immediately, freeing the subscription task

                if (_subscriptionTask != null && waitForSubscriptionTask)
                {
                    try
                    {
                        if (await _subscriptionTask.WaitWithTimeout(TimeSpan.FromSeconds(60)).ConfigureAwait(false) == false)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Subscription worker for '{SubscriptionName}' wasn't done after 60 seconds, cannot hold subscription disposal any longer.");
                        }
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

        public Task Run(Action<TBatch> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (null, processDocuments);
            return RunInternalAsync(ct);
        }

        public Task Run(Func<TBatch, Task> processDocuments, CancellationToken ct = default)
        {
            if (processDocuments == null)
                throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (processDocuments, null);
            return RunInternalAsync(ct);
        }

        internal Task RunInternalAsync(CancellationToken ct)
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException("The subscription is already running");

            if (ct != default)
            {
                using (var old = _processingCts)
                {
                    _processingCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                }
            }

            return _subscriptionTask = RunSubscriptionAsync();
        }

        protected ServerNode _redirectNode;
        protected RequestExecutor _subscriptionLocalRequestExecutor;

        public string CurrentNodeTag => _redirectNode?.ClusterTag;
        public string SubscriptionName => _options?.SubscriptionName;
        internal int? SubscriptionTcpVersion;

        internal bool ShouldUseCompression()
        {
            bool compressionSupport = false;
#if NETCOREAPP3_1_OR_GREATER
            var version = SubscriptionTcpVersion ?? TcpConnectionHeaderMessage.SubscriptionTcpVersion;
            if (version >= 53_000 && (GetRequestExecutor().Conventions.DisableTcpCompression == false))
                compressionSupport = true;
#endif
            return compressionSupport;
        }

        internal async Task<Stream> ConnectToServerAsync(CancellationToken token)
        {
            var command = new GetTcpInfoForRemoteTaskCommand("Subscription/" + _dbName, _dbName, _options?.SubscriptionName, verifyDatabase: true);

            var requestExecutor = GetRequestExecutor();

            await TrySetRedirectNodeOnConnectToServerAsync().ConfigureAwait(false);

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
                        tcpInfo = await LegacyTryGetTcpInfoAsync(requestExecutor, context, _redirectNode, token).ConfigureAwait(false);
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
                        tcpInfo = await LegacyTryGetTcpInfoAsync(requestExecutor, context, token).ConfigureAwait(false);
                    }
                }

                var result = await TcpUtils.ConnectSecuredTcpSocket(
                    tcpInfo,
                    requestExecutor.Certificate,
#if !NETSTANDARD
                    null,
#endif
                    TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    NegotiateProtocolVersionForSubscriptionAsync,
                    context,
                    _options?.ConnectionStreamTimeout,
                    null
#if !NETSTANDARD
                    ,
                    token
#endif
                ).ConfigureAwait(false);

                _tcpClient = result.TcpClient;
                _stream = new StreamWithTimeout(result.Stream);
                _supportedFeatures = result.SupportedFeatures;

                _tcpClient.NoDelay = true;
                _tcpClient.SendBufferSize = _options?.SendBufferSizeInBytes ?? SubscriptionWorkerOptions.DefaultSendBufferSizeInBytes;
                _tcpClient.ReceiveBufferSize = _options?.ReceiveBufferSizeInBytes ?? SubscriptionWorkerOptions.DefaultReceiveBufferSizeInBytes;

                if (_supportedFeatures.ProtocolVersion <= 0)
                {
                    throw new InvalidOperationException(
                        $"{_options.SubscriptionName}: TCP negotiation resulted with an invalid protocol version:{_supportedFeatures.ProtocolVersion}");
                }

#if !NETSTANDARD
                if (_supportedFeatures.DataCompression)
                    _stream = new ReadWriteCompressedStream(_stream);
#endif

                using (var optionsJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_options, context))
                {
                    await optionsJson.WriteJsonToAsync(_stream, token).ConfigureAwait(false);
                    await _stream.FlushAsync(token).ConfigureAwait(false);
                }

                SetLocalRequestExecutor(command.RequestedNode.Url, requestExecutor.Certificate);

                return _stream;
            }
        }

        private async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateProtocolVersionForSubscriptionAsync(string chosenUrl, TcpConnectionInfo tcpInfo, Stream stream, JsonOperationContext context, List<string> _)
        {
            var parameters = new AsyncTcpNegotiateParameters
            {
                Database = _dbName,
                Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                Version = SubscriptionTcpVersion ?? TcpConnectionHeaderMessage.SubscriptionTcpVersion,
                ReadResponseAndGetVersionCallbackAsync = ReadServerResponseAndGetVersionAsync,
                DestinationNodeTag = CurrentNodeTag,
                DestinationUrl = chosenUrl,
                DestinationServerId = tcpInfo.ServerId,
                LicensedFeatures = new LicensedFeatures
                {
                    DataCompression = ShouldUseCompression()
                }
            };

            return await TcpNegotiation.NegotiateProtocolVersionAsync(context, stream, parameters).ConfigureAwait(false);
        }

        private async Task<TcpConnectionInfo> LegacyTryGetTcpInfoAsync(RequestExecutor requestExecutor, JsonOperationContext context, CancellationToken token)
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

        private async Task<TcpConnectionInfo> LegacyTryGetTcpInfoAsync(RequestExecutor requestExecutor, JsonOperationContext context, ServerNode node, CancellationToken token)
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

        private async ValueTask<TcpConnectionHeaderMessage.NegotiationResponse> ReadServerResponseAndGetVersionAsync(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, Stream stream, string destinationUrl)
        {
            //Reading reply from server
            using (var response = await context.ReadForMemoryAsync(stream, "Subscription/tcp-header-response").ConfigureAwait(false))
            {
                var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);

                switch (reply.Status)
                {
                    case TcpConnectionStatus.Ok:
                        return new TcpConnectionHeaderMessage.NegotiationResponse
                        {
                            Version = reply.Version,
                            LicensedFeatures = reply.LicensedFeatures
                        };

                    case TcpConnectionStatus.AuthorizationFailed:
                        throw new AuthorizationException($"Cannot access database {_dbName} because " + reply.Message);

                    case TcpConnectionStatus.TcpVersionMismatch:
                        if (reply.Version != TcpNegotiation.OutOfRangeStatus)
                        {
                            return new TcpConnectionHeaderMessage.NegotiationResponse
                            {
                                Version = reply.Version,
                                LicensedFeatures = reply.LicensedFeatures
                            };
                        }
                        //Kindly request the server to drop the connection
                        await SendDropMessageAsync(context, writer, reply).ConfigureAwait(false);
                        throw new InvalidOperationException($"Can't connect to database {_dbName} because: {reply.Message}");

                    case TcpConnectionStatus.InvalidNetworkTopology:
                        throw new InvalidNetworkTopologyException($"Failed to connect to url {destinationUrl} because: {reply.Message}");
                }

                return new TcpConnectionHeaderMessage.NegotiationResponse
                {
                    Version = reply.Version,
                    LicensedFeatures = reply.LicensedFeatures
                };
            }
        }

        private async ValueTask SendDropMessageAsync(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, TcpConnectionHeaderResponse reply)
        {
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = _dbName,
                [nameof(TcpConnectionHeaderMessage.Operation)] = TcpConnectionHeaderMessage.OperationTypes.Drop.ToString(),
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = TcpConnectionHeaderMessage.GetOperationTcpVersion(TcpConnectionHeaderMessage.OperationTypes.Drop),
                [nameof(TcpConnectionHeaderMessage.Info)] =
                    $"Couldn't agree on subscription TCP version ours:{TcpConnectionHeaderMessage.SubscriptionTcpVersion} theirs:{reply.Version}"
            });

            await writer.FlushAsync().ConfigureAwait(false);
        }

        private void AssertConnectionState(SubscriptionConnectionServerMessage connectionStatus)
        {
            if (connectionStatus.Type == SubscriptionConnectionServerMessage.MessageType.Error)
            {
                if (connectionStatus.Exception.Contains(nameof(DatabaseDoesNotExistException))/* || connectionStatus.Exception.Contains("unloaded and locked by DeleteDatabase")*/)
                    DatabaseDoesNotExistException.ThrowWithMessage(_dbName, connectionStatus.Message);
                else if (connectionStatus.Exception.Contains(nameof(NotSupportedInShardingException)))
                    throw new NotSupportedInShardingException(connectionStatus.Message);
                else if (connectionStatus.Exception.Contains(nameof(DatabaseDisabledException)))
                    throw new DatabaseDisabledException(connectionStatus.Message);
            }

            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
            {
                var message = "Server returned illegal type message when expecting connection status, was: " + connectionStatus.Type;

                if (connectionStatus.Type == SubscriptionConnectionServerMessage.MessageType.Error)
                    message += $". Exception: {connectionStatus.Exception}";

                throw new SubscriptionMessageTypeException(message);
            }

            switch (connectionStatus.Status)
            {
                case SubscriptionConnectionServerMessage.ConnectionStatus.Accepted:
                    break;

                case SubscriptionConnectionServerMessage.ConnectionStatus.InUse:
                    throw new SubscriptionInUseException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it's in use and the connection strategy is {_options.Strategy}");
                case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                    bool closedWhenNoDocsLeft = false;
                    bool canReconnect = false;
                    connectionStatus.Data?.TryGet(nameof(SubscriptionClosedException.NoDocsLeft), out closedWhenNoDocsLeft);
                    connectionStatus.Data?.TryGet(nameof(SubscriptionClosedException.CanReconnect), out canReconnect);
                    throw new SubscriptionClosedException($"Subscription With Id '{_options.SubscriptionName}' was closed.  " + connectionStatus.Exception, canReconnect, closedWhenNoDocsLeft);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Invalid:
                    throw new SubscriptionInvalidStateException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it is in invalid state. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it does not exist. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Redirect:
                    if (_options.Strategy == SubscriptionOpeningStrategy.WaitForFree)
                    {
                        if (connectionStatus.Data != null && connectionStatus.Data.TryGetMember(
                                nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RegisterConnectionDurationInTicks), out object registerConnectionDurationInTicksObject))
                        {
                            if (registerConnectionDurationInTicksObject is long registerConnectionDurationInTicks)
                            {
                                if (TimeSpan.FromTicks(registerConnectionDurationInTicks) >= _options.MaxErroneousPeriod)
                                {
                                    // this worker connection Waited For Free for more than MaxErroneousPeriod
                                    _lastConnectionFailure = null;
                                }
                            }
                        }
                    }

                    var appropriateNode = connectionStatus.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                    var currentNode = connectionStatus.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.CurrentTag)]?.ToString();
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
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be processed by current node '{currentNode}', it will be redirected to {appropriateNode}]{Environment.NewLine}Reasons:{string.Join(Environment.NewLine, reasonsDictionary.Select(x => $"{x.Key}:{x.Value}"))}",
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

                var contextPool = GetRequestExecutor().ContextPool;

                using (contextPool.AllocateOperationContext(out JsonOperationContext context))
                using (context.GetMemoryBuffer(out var buffer))
                using (var tcpStream = await ConnectToServerAsync(_processingCts.Token).ConfigureAwait(false))
                {
                    _processingCts.Token.ThrowIfCancellationRequested();
                    var tcpStreamCopy = tcpStream;
                    using (contextPool.AllocateOperationContext(out JsonOperationContext handshakeContext))
                    {
                        var connectionStatus = await ReadNextObjectAsync(handshakeContext, tcpStreamCopy, buffer).ConfigureAwait(false);
                        if (_processingCts.IsCancellationRequested)
                            return;

                        if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus ||
                            connectionStatus.Status != SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                            AssertConnectionState(connectionStatus);
                    }
                    _lastConnectionFailure = null;
                    if (_processingCts.IsCancellationRequested)
                        return;

                    OnEstablishedSubscriptionConnection?.Invoke();

                    await ProcessSubscriptionInternalAsync(contextPool, tcpStreamCopy, buffer, context).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                if (_disposed == false)
                    throw;

                // otherwise this is thrown when shutting down,
                // it isn't an error, so we don't need to treat it as such
            }
        }

        private async Task ProcessSubscriptionInternalAsync(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer, JsonOperationContext context)
        {
            Task notifiedSubscriber = Task.CompletedTask;

            try
            {
                var batch = CreateEmptyBatch();

                while (_processingCts.IsCancellationRequested == false)
                {
                    var incomingBatch = await PrepareBatchAsync(contextPool, tcpStreamCopy, buffer, batch, notifiedSubscriber).ConfigureAwait(false);

                    notifiedSubscriber = Task.Run(async () => // the 2'nd thread
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
                                    _logger.Info($"Subscription '{_options.SubscriptionName}'. Subscriber threw an exception on document batch", ex);
                                }

                                HandleSubscriberError(ex);
                            }
                        }

                        await SendAckAsync(batch, tcpStreamCopy, context, _processingCts.Token).ConfigureAwait(false);
                    });
                }
            }
            finally
            {
                try
                {
                    if (notifiedSubscriber is { IsCompleted: false })
                    {
                        await notifiedSubscriber.WaitWithTimeout(TimeSpan.FromSeconds(15)).ConfigureAwait(false);
                    }
                }
                catch
                {
                    // ignored
                }
            }
        }

        protected virtual TimeSpan GetTimeToWaitBeforeConnectionRetry() => _options.TimeToWaitBeforeConnectionRetry;

        protected virtual void HandleSubscriberError(Exception ex)
        {
            if (_options.IgnoreSubscriberErrors == false)
                throw new SubscriberErrorException($"Subscriber threw an exception in subscription '{_options.SubscriptionName}'", ex);
        }

        internal virtual async Task<BatchFromServer> PrepareBatchAsync(JsonContextPool contextPool, Stream tcpStreamCopy, JsonOperationContext.MemoryBuffer buffer, TBatch batch, Task notifiedSubscriber)
        {
            // start reading next batch from server on 1'st thread (can be before client started processing)
            var readFromServer = ReadSingleSubscriptionBatchFromServerAsync(contextPool, tcpStreamCopy, buffer, batch);

            try
            {
                // wait for the subscriber to complete processing on 2'nd thread
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

            BatchFromServer incomingBatch = await readFromServer.ConfigureAwait(false); // wait for batch reading to end

            try
            {
                _processingCts.Token.ThrowIfCancellationRequested();

                await batch.InitializeAsync(incomingBatch).ConfigureAwait(false);
            }
            catch (Exception)
            {
                try
                {
                    using (incomingBatch.ReturnContext)
                    {
                    }
                }
                catch (Exception)
                {
                    // nothing to be done here
                }

                throw;
            }

            return incomingBatch;
        }

        internal async Task<BatchFromServer> ReadSingleSubscriptionBatchFromServerAsync(JsonContextPool contextPool, Stream tcpStream,
            JsonOperationContext.MemoryBuffer buffer, TBatch batch)
        {
            var incomingBatch = new List<SubscriptionConnectionServerMessage>();
            var includes = new List<BlittableJsonReaderObject>();
            var counterIncludes = new List<(BlittableJsonReaderObject Includes, Dictionary<string, string[]> IncludedCounterNames)>();
            var timeSeriesIncludes = new List<BlittableJsonReaderObject>();
            IDisposable returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);
            try
            {
                bool endOfBatch = false;
                while (endOfBatch == false && _processingCts.IsCancellationRequested == false)
                {
                    SubscriptionConnectionServerMessage receivedMessage = await ReadNextObjectAsync(context, tcpStream, buffer).ConfigureAwait(false);
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
                            if (batch != null)
                            {
                                var onAfterAcknowledgment = AfterAcknowledgment;
                                if (onAfterAcknowledgment != null)
                                    await onAfterAcknowledgment(batch).ConfigureAwait(false);
                                batch.Items.Clear();
                            }

                            incomingBatch.Clear();
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
            }
            catch (Exception)
            {
                returnContext?.Dispose();
                throw;
            }

            return new BatchFromServer
            {
                Messages = incomingBatch,
                ReturnContext = returnContext,
                Context = context,
                Includes = includes,
                CounterIncludes = counterIncludes,
                TimeSeriesIncludes = timeSeriesIncludes
            };
        }

        private static void ThrowInvalidServerResponse(SubscriptionConnectionServerMessage receivedMessage)
        {
            throw new ArgumentException($"Unrecognized message '{receivedMessage.Type}' type received from server");
        }

        private static void ThrowSubscriptionError(SubscriptionConnectionServerMessage receivedMessage)
        {
            throw new InvalidOperationException($"Connection terminated by server. Exception: {receivedMessage.Exception ?? "None"}");
        }

        internal async Task<SubscriptionConnectionServerMessage> ReadNextObjectAsync(JsonOperationContext context, Stream stream, JsonOperationContext.MemoryBuffer buffer)
        {
            if (_processingCts.IsCancellationRequested || _tcpClient.Connected == false)
                return null;

            if (_disposed) //if we are disposed, nothing to do...
                return null;

            try
            {
                var blittable = await context.ParseToMemoryAsync(stream, "Subscription/next/object", BlittableJsonDocumentBuilder.UsageMode.None, buffer,
                        token: _processingCts.Token)
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

        protected virtual async Task SendAckAsync(TBatch batch, Stream stream, JsonOperationContext context, CancellationToken token)
        {
            try
            {
                await SendAckInternalAsync(batch, stream, context, token).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                //if this happens, this means we are disposing, so don't care..
                //(this piece of code happens asynchronously to external using(tcpStream) statement)
            }
        }

        protected async Task SendAckInternalAsync(TBatch batch, Stream stream, JsonOperationContext context, CancellationToken token)
        {
            if (stream != null) //possibly prevent ObjectDisposedException
            {
                var message = new SubscriptionConnectionClientMessage
                {
                    ChangeVector = batch.LastSentChangeVectorInBatch,
                    Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
                };

                using (var messageJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(message, context))
                {
                    await messageJson.WriteJsonToAsync(stream, token).ConfigureAwait(false);

                    await stream.FlushAsync(token).ConfigureAwait(false);
                }
            }
        }

        internal async Task RunSubscriptionAsync()
        {
            while (_processingCts.IsCancellationRequested == false)
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
                        if (_processingCts.IsCancellationRequested)
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

                        (bool shouldTryToReconnect, _redirectNode) = CheckIfShouldReconnectWorker(ex, AssertLastConnectionFailure, OnUnexpectedSubscriptionError);
                        if (shouldTryToReconnect)
                        {
                            await TimeoutManager.WaitFor(GetTimeToWaitBeforeConnectionRetry(), _processingCts.Token).ConfigureAwait(false);

                            if (_redirectNode == null)
                            {
                                var reqEx = GetRequestExecutor();
                                var curTopology = reqEx.TopologyNodes;
                                if (curTopology != null)
                                {
                                    try
                                    {
                                        await TrySetRedirectNode(reqEx, curTopology).ConfigureAwait(false);
                                        if (_redirectNode == null)
                                        {
                                            if (_logger.IsInfoEnabled)
                                                _logger.Info($"Subscription '{_options.SubscriptionName}'. Cannot set redirect node, will try to connect anyway.", ex);
                                        }
                                        else
                                        {
                                            if (_logger.IsInfoEnabled)
                                                _logger.Info($"Subscription '{_options.SubscriptionName}'. Will modify redirect node from null to {_redirectNode.ClusterTag}", ex);
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        // will let topology to decide
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info($"Subscription '{_options.SubscriptionName}'. Could not select the redirect node will keep it null.", e);
                                    }
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
                        {
                            throw;
                        }

                        throw new AggregateException(new[] { e }.Concat(_recentExceptions));
                    }
                }
            }
        }

        protected virtual async Task TrySetRedirectNode(RequestExecutor reqEx, IReadOnlyList<ServerNode> curTopology)
        {
            var nextNodeIndex = (_forcedTopologyUpdateAttempts++) % curTopology.Count;
            (_, _redirectNode) = await reqEx.GetRequestedNode(curTopology[nextNodeIndex].ClusterTag, throwIfContainsFailures: true).ConfigureAwait(false);
        }

        internal DateTime? _lastConnectionFailure;
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

        protected virtual (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) CheckIfShouldReconnectWorker(Exception ex, Action assertLastConnectionFailure, Action<Exception> onUnexpectedSubscriptionError, bool throwOnRedirectNodeNotFound = true)
        {
            if (ex is AggregateException ae)
            {
                foreach (var exception in ae.InnerExceptions)
                {
                    if (CheckIfShouldReconnectWorker(exception, assertLastConnectionFailure, onUnexpectedSubscriptionError).ShouldTryToReconnect)
                    {
                        return (true, _redirectNode);
                    }
                }

                return HandleAggregateException();
            }

            switch (ex)
            {
                case SubscriptionDoesNotBelongToNodeException se:
                    var requestExecutor = GetRequestExecutor();

                    if (se.AppropriateNode == null)
                    {
                        assertLastConnectionFailure?.Invoke();
                        return (true, null);
                    }

                    ServerNode nodeToRedirectTo = null;
                    if (requestExecutor.TopologyNodes != null)
                        nodeToRedirectTo = requestExecutor.TopologyNodes?.FirstOrDefault(x => x.ClusterTag == se.AppropriateNode);
                    if (nodeToRedirectTo == null && throwOnRedirectNodeNotFound)
                    {
                        throw new AggregateException(ex,
                            new InvalidOperationException($"Could not redirect to {se.AppropriateNode}, because it was not found in local topology, even after retrying"));
                    }

                    return (true, nodeToRedirectTo);

                case DatabaseDisabledException:
                case AllTopologyNodesDownException:
                    assertLastConnectionFailure?.Invoke();
                    return (true, _redirectNode);

                case NodeIsPassiveException _:
                    // if we failed to talk to a node, we'll forget about it and let the topology to
                    // redirect us to the current node
                    return (true, null);
                case SubscriptionChangeVectorUpdateConcurrencyException subscriptionChangeVectorUpdateConcurrencyException:
                    return HandleSubscriptionChangeVectorUpdateConcurrencyException(subscriptionChangeVectorUpdateConcurrencyException);

                case SubscriptionClosedException sce:
                    return HandleSubscriptionClosedException(sce);

                case SubscriptionMessageTypeException _:
                    goto default;

                case SubscriptionInUseException _:
                case SubscriptionDoesNotExistException _:
                case SubscriptionInvalidStateException _:
                case DatabaseDoesNotExistException _:
                case AuthorizationException _:
                case SubscriberErrorException _:
                case NotSupportedInShardingException _:
                    return HandleShouldNotTryToReconnect();

                case RavenException re:
                    if (re.InnerException is HttpRequestException or TimeoutException)
                    {
                        goto default;
                    }

                    return HandleShouldNotTryToReconnect();
                default:
                    onUnexpectedSubscriptionError?.Invoke(ex);
                    assertLastConnectionFailure?.Invoke();
                    return (true, _redirectNode);
            }
        }

        protected virtual (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleShouldNotTryToReconnect()
        {
            _processingCts.Cancel();
            return (false, _redirectNode);
        }

        protected virtual (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleAggregateException()
        {
            return (false, _redirectNode);
        }

        protected virtual (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleSubscriptionChangeVectorUpdateConcurrencyException(SubscriptionChangeVectorUpdateConcurrencyException subscriptionChangeVectorUpdateConcurrencyException)
        {
            return (true, _redirectNode);
        }

        protected virtual (bool ShouldTryToReconnect, ServerNode NodeRedirectTo) HandleSubscriptionClosedException(SubscriptionClosedException sce)
        {
            if (sce.CanReconnect)
                return (true, _redirectNode);

            _processingCts?.Cancel();
            return (false, _redirectNode);
        }

        protected void CloseTcpClient()
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

        protected abstract RequestExecutor GetRequestExecutor();

        protected abstract void SetLocalRequestExecutor(string url, X509Certificate2 cert);

        protected abstract TBatch CreateEmptyBatch();

        protected abstract Task TrySetRedirectNodeOnConnectToServerAsync();

        public event Action<AbstractSubscriptionWorker<TBatch, TType>> OnDisposed = delegate { };

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal bool SimulateUnexpectedException;
        }
    }
}
