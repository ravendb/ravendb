// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Tcp;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Documents.Subscriptions
{
    public class SubscriptionBatch<T>
    {
        /// <summary>
        /// Represents a single item in a subscription batch results. This class should be used only inside the subscription's Run delegate, using it outside this scope might cause unexpected behavior.
        /// </summary>
        public struct Item
        {
            private T _result;
            public string ExceptionMessage { get; internal set; }
            public string Id { get; internal set; }
            public string ChangeVector { get; internal set; }

            private void ThrowItemProcessException()
            {
                throw new InvalidOperationException($"Failed to process document {Id} with Change Vector {ChangeVector} because:{Environment.NewLine}{ExceptionMessage}");
            }

            public T Result
            {
                get
                {
                    if (ExceptionMessage != null)
                        ThrowItemProcessException();

                    return _result;
                }
                internal set => _result = value;
            }

            public BlittableJsonReaderObject RawResult { get; internal set; }
            public BlittableJsonReaderObject RawMetadata { get; internal set; }

            private IMetadataDictionary _metadata;
            public IMetadataDictionary Metadata => _metadata ?? (_metadata = new MetadataAsDictionary(RawMetadata));
        }

        public int NumberOfItemsInBatch => Items?.Count??0;

        private readonly RequestExecutor _requestExecutor;
        private readonly IDocumentStore _store;
        private readonly string _dbName;
        private readonly Logger _logger;
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;

        public List<Item> Items { get; } = new List<Item>();

        public IDocumentSession OpenSession()
        {
            return _store.OpenSession(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return _store.OpenAsyncSession(new SessionOptions
            {
                Database = _dbName,
                RequestExecutor = _requestExecutor
            });
        }

        public SubscriptionBatch(RequestExecutor requestExecutor, IDocumentStore store, string dbName, Logger logger)
        {
            _requestExecutor = requestExecutor;
            _store = store;
            _dbName = dbName;
            _logger = logger;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions, entity => throw new InvalidOperationException("Shouldn't be generating new ids here"));
        }


        internal string Initialize(List<SubscriptionConnectionServerMessage> batch)
        {
            Items.Capacity = Math.Max(Items.Capacity, batch.Count);
            Items.Clear();
            string lastReceivedChangeVector = null;

            foreach (var item in batch)
            {

                BlittableJsonReaderObject metadata;
                var curDoc = item.Data;

                if (curDoc.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                    ThrowRequired("@metadata field");
                if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                    ThrowRequired("@id field");
                if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false ||
                    changeVector == null)
                    ThrowRequired("@change-vector field");
                else
                    lastReceivedChangeVector = changeVector;

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Got {id} (change vector: [{lastReceivedChangeVector}], size {curDoc.Size}");
                }

                var instance = default(T);

                if (item.Exception == null)
                {
                    if (typeof(T) == typeof(BlittableJsonReaderObject))
                    {
                        instance = (T)(object)curDoc;
                    }
                    else
                    {
                        instance = (T)EntityToBlittable.ConvertToEntity(typeof(T), id, curDoc, _requestExecutor.Conventions);
                    }

                    if (string.IsNullOrEmpty(id) == false)
                        _generateEntityIdOnTheClient.TrySetIdentity(instance, id);
                }

                Items.Add(new Item
                {
                    ChangeVector = changeVector,
                    Id = id,
                    RawResult = curDoc,
                    RawMetadata = metadata,
                    Result = instance,
                    ExceptionMessage = item.Exception
                });
            }
            return lastReceivedChangeVector;
        }

        private static void ThrowRequired(string name)
        {
            throw new InvalidOperationException("Document must have a " + name);
        }
    }

    public class Subscription<T> : IAsyncDisposable, IDisposable where T : class
    {
        public delegate Task AfterAcknowledgmentAction(SubscriptionBatch<T> batch);

        private readonly Logger _logger;
        private readonly IDocumentStore _store;
        private readonly string _dbName;
        private readonly CancellationTokenSource _processingCts = new CancellationTokenSource();
        private readonly SubscriptionConnectionOptions _options;
        private (Func<SubscriptionBatch<T>, Task> Async, Action<SubscriptionBatch<T>> Sync) _subscriber;
        private TcpClient _tcpClient;
        private bool _disposed;
        private Task _subscriptionTask;
        private Stream _stream;

        /// <summary>
        /// allows the user to define stuff that happens after the confirm was received from the server (this way we know we won't
        /// get those documents again)
        /// </summary>
        public event AfterAcknowledgmentAction AfterAcknowledgment;

        internal Subscription(SubscriptionConnectionOptions options, IDocumentStore documentStore, string dbName)
        {
            _options = options;
            _logger = LoggingSource.Instance.GetLogger<Subscription<T>>(dbName);
            if (string.IsNullOrEmpty(options.SubscriptionName))
                throw new ArgumentException("SubscriptionConnectionOptions must specify the SubscriptionName",nameof(options));
            _store = documentStore;
            _dbName = dbName ?? documentStore.Database;


        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool waitForSubscriptionTask)
        {
            if (_disposed)
                return;

            AsyncHelpers.RunSync(() => DisposeAsync(waitForSubscriptionTask));
        }

        public Task DisposeAsync()
        {
            return DisposeAsync(true);
        }

        public async Task DisposeAsync(bool waitForSubscriptionTask)
        {
            try
            {
                if (_disposed)
                    return;

                _disposed = true;
                _processingCts.Cancel();

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

        public Task Run(Action<SubscriptionBatch<T>> processDocuments)
        {
            if (processDocuments == null) throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (null, processDocuments);
            return Run();
        }

        public Task Run(Func<SubscriptionBatch<T>, Task> processDocuments)
        {
            if (processDocuments == null) throw new ArgumentNullException(nameof(processDocuments));
            _subscriber = (processDocuments, null);
            return Run();
        }

        private Task Run()
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException("The subscription is already running");

            return _subscriptionTask = RunSubscriptionAsync();

        }

        private ServerNode _redirectNode;
        private RequestExecutor _subscriptionLocalRequestExecutor;

        public string CurrentNodeTag => _redirectNode?.ClusterTag;
        public string SubscriptionName => _options?.SubscriptionName;

        private async Task<Stream> ConnectToServer()
        {
            var command = new GetTcpInfoCommand("Subscription/" + _dbName, _dbName);

            JsonOperationContext context;
            var requestExecutor = _store.GetRequestExecutor(_dbName);

            using (requestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                if (_redirectNode != null)
                {
                    try
                    {
                        await requestExecutor.ExecuteAsync(_redirectNode, null, context, command, shouldRetry: false).ConfigureAwait(false);
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
                    await requestExecutor.ExecuteAsync(command, context).ConfigureAwait(false);
                }

                _tcpClient = await TcpUtils.ConnectAsync(command.Result.Url, requestExecutor.DefaultTimeout).ConfigureAwait(false);
                _tcpClient.NoDelay = true;
                _tcpClient.SendBufferSize = 32 * 1024;
                _tcpClient.ReceiveBufferSize = 4096;
                _stream = _tcpClient.GetStream();
                _stream = await TcpUtils.WrapStreamWithSslAsync(_tcpClient,command.Result, _store.Certificate).ConfigureAwait(false);

                var databaseName = _dbName ?? _store.Database;
                var serializeObject = JsonConvert.SerializeObject(new TcpConnectionHeaderMessage
                {
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    DatabaseName = databaseName,
                    OperationVersion = TcpConnectionHeaderMessage.SubscriptionTcpVersion
                });
                var header = Encodings.Utf8.GetBytes(serializeObject);

                var options = Encodings.Utf8.GetBytes(JsonConvert.SerializeObject(_options));

                await _stream.WriteAsync(header, 0, header.Length).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
                //Reading reply from server
                using (var response = context.ReadForMemory(_stream, "Subscription/tcp-header-response"))
                {
                    var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                    switch (reply.Status)
                    {
                        case TcpConnectionStatus.Ok:
                            break;
                        case TcpConnectionStatus.AuthorizationFailed:
                            throw new AuthorizationException($"Cannot access database {databaseName} because " + reply.Message);
                        case TcpConnectionStatus.TcpVersionMismatch:
                            throw new InvalidOperationException($"Can't connect to database {databaseName} because: {reply.Message}");
                    }
                        
                }
                await _stream.WriteAsync(options, 0, options.Length).ConfigureAwait(false);

                await _stream.FlushAsync().ConfigureAwait(false);

                _subscriptionLocalRequestExecutor?.Dispose();
                _subscriptionLocalRequestExecutor = RequestExecutor.CreateForSingleNodeWithoutConfigurationUpdates(command.RequestedNode.Url, _dbName, requestExecutor.Certificate, _store.Conventions);

                return _stream;
            }
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
                    throw new SubscriptionClosedException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it was closed.  " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Invalid:
                    throw new SubscriptionInvalidStateException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it is in invalid state. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be opened, because it does not exist. " + connectionStatus.Exception);
                case SubscriptionConnectionServerMessage.ConnectionStatus.Redirect:
                    var appropriateNode = connectionStatus.Data?[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]?.ToString();
                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription With Id '{_options.SubscriptionName}' cannot be processed by current node, it will be redirected to {appropriateNode}"
                    )
                    {
                        AppropriateNode = appropriateNode
                    };
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
                using (var buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                {
                    using (var tcpStream = await ConnectToServer().ConfigureAwait(false))
                    {
                        _processingCts.Token.ThrowIfCancellationRequested();
                        JsonOperationContext handshakeContext;
                        var tcpStreamCopy = tcpStream;
                        using (contextPool.AllocateOperationContext(out handshakeContext))
                        {
                            var connectionStatus = await ReadNextObject(handshakeContext, tcpStreamCopy, buffer).ConfigureAwait(false);
                            if (_processingCts.IsCancellationRequested)
                                return;

                            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus ||
                                connectionStatus.Status != SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                                AssertConnectionState(connectionStatus);
                        }

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

                            var lastReceivedChangeVector = batch.Initialize(incomingBatch.Messages);


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
                                            throw new SubscriberErrorException($"Subscriber threw an exception in subscription '{_options.SubscriptionName}'",ex);
                                    }

                                }

                                try
                                {
                                    if (tcpStreamCopy != null) //possibly prevent ObjectDisposedException
                                    {
                                        SendAck(lastReceivedChangeVector, tcpStreamCopy);
                                    }
                                }
                                catch (ObjectDisposedException)
                                {
                                    //if this happens, this means we are disposing, so don't care..
                                    //(this peace of code happens asynchronously to external using(tcpStream) statement)
                                }
                            });
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this is thrown when shutting down, it
                // isn't an error, so we don't need to treat
                // it as such
            }
        }

        private async Task<(List<SubscriptionConnectionServerMessage> Messages, IDisposable ReturnContext)> ReadSingleSubscriptionBatchFromServer(JsonContextPool contextPool, Stream tcpStream, JsonOperationContext.ManagedPinnedBuffer buffer, SubscriptionBatch<T> batch)
        {
            JsonOperationContext context;
            var incomingBatch = new List<SubscriptionConnectionServerMessage>();
            var returnContext = contextPool.AllocateOperationContext(out context);
            bool endOfBatch = false;
            while (endOfBatch == false && _processingCts.IsCancellationRequested == false)
            {
                var receivedMessage = await ReadNextObject(context, tcpStream, buffer).ConfigureAwait(false);
                if (receivedMessage == null || _processingCts.IsCancellationRequested)
                    break;

                switch (receivedMessage.Type)
                {
                    case SubscriptionConnectionServerMessage.MessageType.Data:
                        incomingBatch.Add(receivedMessage);
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
            return (incomingBatch, returnContext);
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

        private async Task<SubscriptionConnectionServerMessage> ReadNextObject(JsonOperationContext context, Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer)
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


        private void SendAck(string lastReceivedChangeVector, Stream networkStream)
        {
            var ack = Encodings.Utf8.GetBytes(JsonConvert.SerializeObject(new SubscriptionConnectionClientMessage
            {
                ChangeVector = lastReceivedChangeVector,
                Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
            }));

            networkStream.Write(ack, 0, ack.Length);
            networkStream.Flush();
        }

        private async Task RunSubscriptionAsync()
        {
            while (_processingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    CloseTcpClient();
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Subscription '{_options.SubscriptionName}'. Connecting to server...");
                    }

                    await ProcessSubscriptionAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    try
                    {
                        if (_processingCts.Token.IsCancellationRequested)
                            return;

                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info(
                                $"Subscription '{_options.SubscriptionName}'. Pulling task threw the following exception", ex);
                        }
                        if (ShouldTryToReconnect(ex))
                        {
                            await TimeoutManager.WaitFor(_options.TimeToWaitBeforeConnectionRetry).ConfigureAwait(false);
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

                        throw new AggregateException(e, ex);
                    }
                }
            }
        }

        private bool ShouldTryToReconnect(Exception ex)
        {
            switch (ex)
            {
                case SubscriptionDoesNotBelongToNodeException se:
                    var requestExecutor = _store.GetRequestExecutor(_dbName);
                    var nodeToRedirectTo = requestExecutor.TopologyNodes
                        .FirstOrDefault(x => x.ClusterTag == se.AppropriateNode);
                    _redirectNode = nodeToRedirectTo ?? throw new AggregateException(ex,
                                        new InvalidOperationException($"Could not redirect to {se.AppropriateNode}, because it was not found in local topology, even after retrying"));

                    return true;
                case SubscriptionInUseException _:
                case SubscriptionDoesNotExistException _:
                case SubscriptionClosedException _:
                case SubscriptionInvalidStateException _:
                case DatabaseDoesNotExistException _:
                case AuthorizationException _:
                case AllTopologyNodesDownException _:
                case SubscriberErrorException _:
                case RavenException _:
                    _processingCts.Cancel();
                    return false;
                default:
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

        public event Action<Subscription<T>> OnDisposed = delegate { };
    }
}
