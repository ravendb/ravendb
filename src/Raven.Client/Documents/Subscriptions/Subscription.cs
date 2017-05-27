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
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Client.Documents.Subscriptions
{
    public delegate void BeforeBatch();

    public delegate void AfterBatch(int documentsProcessed);

    public delegate void BeforeAcknowledgment();

    public delegate void AfterAcknowledgment();

    public delegate void SubscriptionConnectionInterrupted(Exception ex, bool willReconnect);

    public delegate void ConnectionEstablished();

    public class Subscription<T> : IObservable<T>, IAsyncDisposable, IDisposable where T : class
    {
        private readonly Logger _logger;
        private readonly IDocumentStore _store;
        private readonly DocumentConventions _conventions;
        private readonly string _dbName;
        private readonly CancellationTokenSource _processingCts = new CancellationTokenSource();
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;
        private readonly SubscriptionConnectionOptions _options;
        private readonly List<IObserver<T>> _subscribers = new List<IObserver<T>>();
        private TcpClient _tcpClient;
        private bool _completed;
        private bool _disposed;
        private Task _subscriptionTask;
        private Stream _stream;
        private readonly TaskCompletionSource<object> _disposedTask = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        ///     It indicates if the subscription is in errored state because one of subscribers threw an exception.
        /// </summary>
        public bool IsErroredBecauseOfSubscriber { get; private set; }

        /// <summary>
        ///     The last exception thrown by one of subscribers.
        /// </summary>
        public Exception LastSubscriberException { get; private set; }

        /// <summary>
        /// Task that will be completed when the subscription connection will close and self dispose, or error if subscription connection is entirely interrupted
        /// </summary>
        public Task SubscriptionLifetimeTask { get; }

        /// <summary>
        ///     It determines if the subscription connection is closed.
        /// </summary>
        public bool IsConnectionClosed { get; private set; }

        /// <summary>
        /// Called before received batch starts to be processed by subscribers
        /// </summary>
        public event BeforeBatch BeforeBatch = delegate { };

        /// <summary>
        /// Called after received batch finished being processed by subscribers
        /// </summary>
        public event AfterBatch AfterBatch = delegate { };

        /// <summary>
        /// Called before subscription progress is being stored to the DB
        /// </summary>
        public event BeforeAcknowledgment BeforeAcknowledgment = delegate { };

        /// <summary>
        /// Called when subscription connection is interrupted. The error passed will describe the reason for the interruption. 
        /// </summary>
        public event SubscriptionConnectionInterrupted SubscriptionConnectionInterrupted = delegate { };

        public event ConnectionEstablished ConnectionEstablished = delegate { };

        internal Subscription(SubscriptionConnectionOptions options, IDocumentStore documentStore,
            DocumentConventions conventions, string dbName)
        {
            _options = options;
            _logger = LoggingSource.Instance.GetLogger<Subscription<T>>(dbName);
            if (string.IsNullOrEmpty(_options.SubscriptionId))
                throw new ArgumentException(
                    "SubscriptionConnectionOptions must specify the SubscriptionId, but was set to zero.",
                    nameof(options));
            _store = documentStore;
            _conventions = conventions;
            _dbName = dbName ?? documentStore.Database;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                entity => throw new InvalidOperationException("Shouldn't be generating new ids here"));

            SubscriptionLifetimeTask = _taskCompletionSource.Task;
        }

        public string SubscriptionId => _options.SubscriptionId;

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
#pragma warning disable 4014
                _taskCompletionSource.Task.IgnoreUnobservedExceptions();
#pragma warning restore 4014
                _processingCts.Cancel();
                _disposedTask.TrySetResult(null); // notify the subscription task that we are done

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

                CloseTcpClient(); // we disconnect immediately, freeing the subscription task

                OnCompletedNotification();

                if (_taskCompletionSource.Task.IsCanceled == false && _taskCompletionSource.Task.IsCompleted == false && _taskCompletionSource.Task.IsFaulted == false)
                {
                    _taskCompletionSource.TrySetResult(true);
                }
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error during dispose of subscription", ex);
            }
        }

        public void Subscribe(IObserver<T> observer)
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException(
                    "You can only add observers to a subscriptions before you started it");

            if (IsErroredBecauseOfSubscriber)
                throw new InvalidOperationException(
                    "Subscription encountered errors and stopped. Cannot add any subscriber.");
            _subscribers.Add(observer);
        }

        IDisposable IObservable<T>.Subscribe(IObserver<T> observer)
        {
            Subscribe(observer);

            return new DisposableAction(() => throw new NotSupportedException("Removing subscription is not possible, create a new subscription if you want to change the subscribers."));
        }

        /// <summary>
        /// allows the user to define stuff that happens after the confirm was received from the server (this way we know we won't
        /// get those documents again)
        /// </summary>
        public event AfterAcknowledgment AfterAcknowledgment = delegate { };

        public Task StartAsync()
        {
            if (_subscriptionTask != null)
                return Task.CompletedTask;

            if (_subscribers.Count == 0)
                throw new InvalidOperationException(
                    "No observers has been registered, did you forget to call Subscribe?");

            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            _subscriptionTask = Task.Run(async () =>
            {
                try
                {
                    await RunSubscriptionAsync(tcs);
                }
                catch (Exception e) when (e is OperationCanceledException == false)
                {
                    tcs.TrySetException(e);
                }
                finally
                {
                    if (_processingCts.IsCancellationRequested)
                    {
                        tcs.TrySetCanceled();
                    }
                    else
                    {
                        tcs.TrySetResult(null);
                    }
                }
            });

            return tcs.Task;
        }

        private ServerNode _redirectNode;

        public string CurrentNodeTag => _redirectNode?.ClusterTag;

        private async Task<Stream> ConnectToServer()
        {
            var command = new GetTcpInfoCommand("Subscription/" + _dbName);

            JsonOperationContext context;
            var requestExecutor = _store.GetRequestExecutor(_dbName);

            using (requestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                if (_redirectNode != null)
                {
                    try
                    {
                        await requestExecutor.ExecuteAsync(_redirectNode, context, command, shouldRetry: false).ConfigureAwait(false);

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

                var apiToken = await requestExecutor.GetAuthenticationToken(context, command.RequestedNode).ConfigureAwait(false);
                var uri = new Uri(command.Result.Url);

                _tcpClient = new TcpClient();
                await _tcpClient.ConnectAsync(uri.Host, uri.Port).ConfigureAwait(false);

                _tcpClient.NoDelay = true;
                _tcpClient.SendBufferSize = 32 * 1024;
                _tcpClient.ReceiveBufferSize = 4096;
                _stream = _tcpClient.GetStream();
                _stream = await TcpUtils.WrapStreamWithSslAsync(_tcpClient, command.Result).ConfigureAwait(false);

                var databaseName = _dbName ?? _store.Database;
                var header = Encodings.Utf8.GetBytes(JsonConvert.SerializeObject(new TcpConnectionHeaderMessage
                {
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    DatabaseName = databaseName,
                    AuthorizationToken = apiToken
                }));

                var options = Encodings.Utf8.GetBytes(JsonConvert.SerializeObject(_options));

                await _stream.WriteAsync(header, 0, header.Length).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
                //Reading reply from server
                using (var response = context.ReadForMemory(_stream, "Subscription/tcp-header-response"))
                {
                    var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                    switch (reply.Status)
                    {
                        case TcpConnectionHeaderResponse.AuthorizationStatus.Forbidden:
                            throw AuthorizationException.Forbidden($"Cannot access database {databaseName} because we got a Forbidden authorization status");
                        case TcpConnectionHeaderResponse.AuthorizationStatus.Success:
                            break;
                        default:
                            throw AuthorizationException.Unauthorized(reply.Status, _dbName);
                    }
                }
                await _stream.WriteAsync(options, 0, options.Length).ConfigureAwait(false);

                await _stream.FlushAsync().ConfigureAwait(false);
                return _stream;
            }
        }

        private void AssertConnectionState(SubscriptionConnectionServerMessage connectionStatus)
        {
            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus)
                throw new Exception("Server returned illegal type message when expecting connection status, was: " +
                                    connectionStatus.Type);

            switch (connectionStatus.Status)
            {
                case SubscriptionConnectionServerMessage.ConnectionStatus.Accepted:
                    break;
                case SubscriptionConnectionServerMessage.ConnectionStatus.InUse:
                    throw new SubscriptionInUseException(
                        $"Subscription With Id {_options.SubscriptionId} cannot be opened, because it's in use and the connection strategy is {_options.Strategy}");
                case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                    throw new SubscriptionClosedException(
                        $"Subscription With Id {_options.SubscriptionId} cannot be opened, because it was closed");
                case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id {_options.SubscriptionId} cannot be opened, because it does not exist");
                case SubscriptionConnectionServerMessage.ConnectionStatus.Redirect:
                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Subscription With Id {_options.SubscriptionId} cannot be proccessed by current node, it will be redirected to {connectionStatus.Data[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)]}")
                    {
                        AppropriateNode = connectionStatus.Data[nameof(SubscriptionConnectionServerMessage.SubscriptionRedirectData.RedirectedTag)].ToString()
                    };
                default:
                    throw new ArgumentException(
                        $"Subscription {_options.SubscriptionId} could not be opened, reason: {connectionStatus.Status}");
            }
        }

        private async Task ProcessSubscriptionAsync(TaskCompletionSource<object> successfullyConnected)
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
                        using (contextPool.AllocateOperationContext(out handshakeContext))
                        {
                            var readObjectTask = ReadNextObject(handshakeContext, tcpStream, buffer);
                            var done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                            if (done == _disposedTask.Task)
                                return;
                            var connectionStatus = await readObjectTask.ConfigureAwait(false);
                            if (_processingCts.IsCancellationRequested)
                                return;

                            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.ConnectionStatus ||
                                connectionStatus.Status != SubscriptionConnectionServerMessage.ConnectionStatus.Accepted)
                                AssertConnectionState(connectionStatus);
                        }

                        ConnectionEstablished();

                        successfullyConnected.TrySetResult(null);

                        if (_processingCts.IsCancellationRequested)
                            return;

                        Task notifiedSubscribers = Task.CompletedTask;

                        while (_processingCts.IsCancellationRequested == false)
                        {
                            BeforeBatch();
                            var incomingBatch = await ReadSingleSubscriptionBatchFromServer(contextPool, tcpStream, buffer);
                            try
                            {
                                await notifiedSubscribers;
                            }
                            catch (Exception)
                            {
                                incomingBatch.returnContext.Dispose();
                                throw;
                            }
                            notifiedSubscribers = Task.Run(() =>
                            {
                                ChangeVectorEntry[] lastReceivedChangeVector = null;
                                // ReSharper disable once AccessToDisposedClosure
                                using (incomingBatch.returnContext)
                                {
                                    foreach (var curDoc in incomingBatch.messages)
                                    {
                                        NotifySubscribers(curDoc.Data, out lastReceivedChangeVector);
                                    }
                                }
                                try
                                {
                                    if (tcpStream != null) //possibly prevent ObjectDisposedException
                                    {
                                        SendAck(lastReceivedChangeVector, tcpStream);
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

            }
            catch (Exception ex)
            {
                SubscriptionConnectionInterrupted(ex, true);
                throw;
            }
        }

        private async Task<(List<SubscriptionConnectionServerMessage> messages, IDisposable returnContext)> ReadSingleSubscriptionBatchFromServer(JsonContextPool contextPool, Stream tcpStream, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            JsonOperationContext context;
            var incomingBatch = new List<SubscriptionConnectionServerMessage>();
            var returnContext = contextPool.AllocateOperationContext(out context);
            bool endOfBatch = false;
            while (endOfBatch == false && _processingCts.IsCancellationRequested == false)
            {
                var readObjectTask = ReadNextObject(context, tcpStream, buffer);

                var done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                if (done == _disposedTask.Task)
                {
                    break;
                }

                var receivedMessage = await readObjectTask.ConfigureAwait(false);
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
                        AfterAcknowledgment();
                        AfterBatch(incomingBatch.Count);
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

        private void NotifySubscribers(BlittableJsonReaderObject curDoc, out ChangeVectorEntry[] lastReceivedChangeVector)
        {
            BlittableJsonReaderObject metadata;
            lastReceivedChangeVector = null;

            if (curDoc.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                ThrowMetadataRequired();
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                ThrowIdRequired();
            if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out BlittableJsonReaderArray changeVectorAsObject) == false || changeVectorAsObject == null)
                ThrowChangeVectorRequired();
            else
                lastReceivedChangeVector = changeVectorAsObject.ToVector();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Got {id} (change vector: [{string.Join(",", lastReceivedChangeVector.Select(x => $"{x.DbId.ToString()}:{x.Etag}"))}] on subscription {_options.SubscriptionId}, size {curDoc.Size}");
            }

            T instance;

            if (typeof(T) == typeof(BlittableJsonReaderObject))
            {
                instance = (T)(object)curDoc;
            }
            else
            {
                instance = (T)EntityToBlittable.ConvertToEntity(typeof(T), id, curDoc, _conventions);
            }

            if (string.IsNullOrEmpty(id) == false)
                _generateEntityIdOnTheClient.TrySetIdentity(instance, id);

            foreach (var subscriber in _subscribers)
            {
                _processingCts.Token.ThrowIfCancellationRequested();
                try
                {
                    subscriber.OnNext(instance);
                }
                catch (Exception ex)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription #{_options.SubscriptionId}. Subscriber threw an exception", ex);
                    }

                    if (_options.IgnoreSubscriberErrors == false)
                    {
                        IsErroredBecauseOfSubscriber = true;
                        LastSubscriberException = ex;
                        SubscriptionConnectionInterrupted(ex, false);
                        _taskCompletionSource.TrySetException(ex);
                        _processingCts.Cancel();

                        try
                        {
                            subscriber.OnError(ex);
                        }
                        catch (Exception)
                        {
                            // can happen if a subscriber doesn't have an onError handler - just ignore it
                        }

                        break;
                    }
                }
            }
        }

        private static void ThrowChangeVectorRequired()
        {
            throw new InvalidOperationException("Document must have a ChangeVector");
        }

        private static void ThrowIdRequired()
        {
            throw new InvalidOperationException("Document must have an id");
        }

        private static void ThrowMetadataRequired()
        {
            throw new InvalidOperationException("Document must have a metadata");
        }

        private void SendAck(ChangeVectorEntry[] lastReceivedChangeVector, Stream networkStream)
        {
            BeforeAcknowledgment();

            var ack = Encodings.Utf8.GetBytes(JsonConvert.SerializeObject(new SubscriptionConnectionClientMessage
            {
                ChangeVector = lastReceivedChangeVector,
                Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
            }));

            networkStream.Write(ack, 0, ack.Length);
            networkStream.Flush();
        }

        private async Task RunSubscriptionAsync(TaskCompletionSource<object> firstConnectionCompleted)
        {
            while (_processingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    CloseTcpClient();
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Subscription #{_options.SubscriptionId}. Connecting to server...");
                    }

                    await ProcessSubscriptionAsync(firstConnectionCompleted).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    try
                    {
                        if (_processingCts.Token.IsCancellationRequested)
                        {
                            SubscriptionConnectionInterrupted(ex, true);
                            firstConnectionCompleted.TrySetResult(false);
                            return;
                        }
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info(
                                $"Subscription #{_options.SubscriptionId}. Pulling task threw the following exception", ex);
                        }
                        if (await TryHandleRejectedConnectionOrDispose(ex).ConfigureAwait(false))
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info($"Connection to subscription #{_options.SubscriptionId} have been shut down because of an error", ex);

                            firstConnectionCompleted.TrySetException(ex);
                            return;
                        }

                        await TimeoutManager.WaitFor(_options.TimeToWaitBeforeConnectionRetry).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        firstConnectionCompleted.TrySetResult(false);
                        firstConnectionCompleted.TrySetException(new AggregateException(ex, e));
                        await DisposeAsync(false).ConfigureAwait(false);
                        return;
                    }
                }
            }
        }

        private async Task<bool> TryHandleRejectedConnectionOrDispose(Exception ex)
        {
            if (ex is SubscriptionInUseException || // another client has connected to the subscription
                ex is SubscriptionDoesNotExistException || // subscription has been deleted meanwhile
                ex is SubscriptionClosedException) // subscription has been booted by another subscription
            {
                IsConnectionClosed = true;
                _taskCompletionSource.TrySetException(ex);
                SubscriptionConnectionInterrupted(ex, false);
                _processingCts.Cancel();

                await DisposeAsync(false).ConfigureAwait(false);

                return true;
            }

            // ReSharper disable once InvertIf
            if (ex is SubscriptionDoesNotBelongToNodeException se)
            {
                var requestExecuter = _store.GetRequestExecutor(_dbName);
                var nodeToRedirectTo = requestExecuter.TopologyNodes
                    .FirstOrDefault(x => x.ClusterTag == se.AppropriateNode);
                if (nodeToRedirectTo == null)
                {
                    nodeToRedirectTo = requestExecuter.TopologyNodes.FirstOrDefault(x => x.ClusterTag == se.AppropriateNode);

                    if (nodeToRedirectTo == null)
                    {
                        SubscriptionConnectionInterrupted(new AggregateException(ex,
                            new InvalidOperationException($"Could not redirect to {se.AppropriateNode}, because it was not found in local topology, even after retrying")), false);
                        return true;
                    }

                }

                _redirectNode = nodeToRedirectTo;
                SubscriptionConnectionInterrupted(ex, true);
            }

            return false;
        }

        private void OnCompletedNotification()
        {
            if (_completed)
                return;

            foreach (var subscriber in _subscribers)
            {
                try
                {
                    subscriber.OnCompleted();
                }
                catch (Exception)
                {
                    // nothing to be done here
                }
            }

            _completed = true;
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
                }
            }
        }

        public void Start()
        {
            AsyncHelpers.RunSync(StartAsync);
        }
    }
}
