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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Security;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Commands;
using Raven.Client.Server.Tcp;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Subscriptions
{
    public delegate void BeforeBatch();

    public delegate void AfterBatch(int documentsProcessed);

    public delegate void BeforeAcknowledgment();

    public delegate void AfterAcknowledgment();

    public delegate void SubscriptionConnectionInterrupted(Exception ex, bool willReconnect);

    public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable where T : class
    {
        private readonly Logger _logger;
        private readonly IDocumentStore _store;
        private readonly DocumentConventions _conventions;
        private readonly string _dbName;
        private readonly CancellationTokenSource _proccessingCts = new CancellationTokenSource();
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
        /// Task that will be completed when the subscription connection will close and self dispose, or errorous if subscription connection is entirely interrupted
        /// </summary>
        public Task SubscriptionLifetimeTask { private set; get; }

        /// <summary>
        ///     It determines if the subscription connection is closed.
        /// </summary>
        public bool IsConnectionClosed { get; private set; }

        /// <summary>
        /// Called before received batch starts to be proccessed by subscribers
        /// </summary>
        public event BeforeBatch BeforeBatch = delegate { };

        /// <summary>
        /// Called after received batch finished being proccessed by subscribers
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



        internal Subscription(SubscriptionConnectionOptions options, IDocumentStore documentStore,
            DocumentConventions conventions, string dbName)
        {
            _options = options;
            _logger = LoggingSource.Instance.GetLogger<Subscription<T>>(dbName);
            if (_options.SubscriptionId == 0)
                throw new ArgumentException(
                    "SubscriptionConnectionOptions must specify the SubscriptionId, but was set to zero.",
                    nameof(options));
            _store = documentStore;
            _conventions = conventions;
            _dbName = dbName;

            _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                entity => throw new InvalidOperationException("Shouldn't be generating new ids here"));

            SubscriptionLifetimeTask = _taskCompletionSource.Task;
        }

        ~Subscription()
        {
            if (_disposed) return;
            try
            {
                CloseTcpClient();
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Subscription {_options.SubscriptionId} was not disposed properly");
            }
            catch
            {
            }
        }

        

        public void Dispose()
        {
            if (_disposed)
                return;

            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (_disposed)
                    return;
                
                _disposed = true;
#pragma warning disable 4014
                _taskCompletionSource.Task.IgnoreUnobservedExceptions();
#pragma warning restore 4014
                _proccessingCts.Cancel();
                _disposedTask.TrySetResult(null); // notify the subscription task that we are done

                if (_subscriptionTask != null && Task.CurrentId != _subscriptionTask.Id)
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

        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (_subscriptionTask != null)
                throw new InvalidOperationException(
                    "You can only add observers to a subscriptions before you started it");

            if (IsErroredBecauseOfSubscriber)
                throw new InvalidOperationException(
                    "Subscription encountered errors and stopped. Cannot add any subscriber.");
            _subscribers.Add(observer);

            // we cannot remove subscriptions dynamically, once we added, it is done
            return new DisposableAction(() => { });
        }

        /// <summary>
        /// allows the user to define stuff that happens after the confirm was recieved from the server (this way we know we won't
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
                finally
                {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    if (_proccessingCts.IsCancellationRequested)
                    {
                        Task.Run(() => tcs.TrySetCanceled());
                    }
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                }
            });

            return tcs.Task;
        }

        private async Task<Stream> ConnectToServer()
        {
            var command = new GetTcpInfoCommand();

            JsonOperationContext context;
            var requestExecuter = _store.GetRequestExecuter(_dbName ?? _store.DefaultDatabase);

            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                await requestExecuter.ExecuteAsync(command, context).ConfigureAwait(false);
                var apiToken = await requestExecuter.GetAuthenticationToken(context, command.RequestedNode).ConfigureAwait(false);
                var uri = new Uri(command.Result.Url);

                await _tcpClient.ConnectAsync(uri.Host, uri.Port).ConfigureAwait(false);

                _tcpClient.NoDelay = true;
                _tcpClient.SendBufferSize = 32 * 1024;
                _tcpClient.ReceiveBufferSize = 4096;
                _stream = _tcpClient.GetStream();
                _stream = await TcpUtils.WrapStreamWithSslAsync(_tcpClient, command.Result).ConfigureAwait(false);

                var header = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new TcpConnectionHeaderMessage
                {
                    Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                    DatabaseName = _dbName ?? _store.DefaultDatabase,
                    AuthorizationToken = apiToken
                }));

                var options = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(_options));

                await _stream.WriteAsync(header, 0, header.Length).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
                //Reading reply from server
                using (var response = context.ReadForMemory(_stream, "Subscription/tcp-header-response"))
                {
                    var reply = JsonDeserializationClient.TcpConnectionHeaderResponse(response);
                    switch (reply.Status)
                    {
                        case TcpConnectionHeaderResponse.AuthorizationStatus.Forbidden:
                            throw AuthorizationException.Forbidden(_store.Url);
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
            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.CoonectionStatus)
                throw new Exception("Server returned illegal type message when excpecting connection status, was: " +
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
                default:
                    throw new ArgumentException(
                        $"Subscription {_options.SubscriptionId} could not be opened, reason: {connectionStatus.Status}");
            }
        }

        private async Task ProccessSubscriptionAsync(TaskCompletionSource<object> successfullyConnected)
        {
            try
            {
                _proccessingCts.Token.ThrowIfCancellationRequested();
                var contextPool = _store.GetRequestExecuter(_dbName).ContextPool;
                using (var buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                {
                    using (var tcpStream = await ConnectToServer().ConfigureAwait(false))
                    {
                        _proccessingCts.Token.ThrowIfCancellationRequested();
                        JsonOperationContext handshakeContext;
                        using (contextPool.AllocateOperationContext(out handshakeContext))
                        {
                            var readObjectTask = ReadNextObject(handshakeContext, tcpStream, buffer);
                            var done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                            if (done == _disposedTask.Task)
                                return;
                            var connectionStatus = await readObjectTask.ConfigureAwait(false);
                            if (_proccessingCts.IsCancellationRequested)
                                return;

                            AssertConnectionState(connectionStatus);
                        }

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        Task.Run(() => successfullyConnected.TrySetResult(null));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed


                        if (_proccessingCts.IsCancellationRequested)
                            return;

                        long lastReceivedEtag = 0;

                        Task notifiedSubscribers = Task.CompletedTask;

                        while (_proccessingCts.IsCancellationRequested == false)
                        {
                            BeforeBatch();
                            var incomingBatch = await ReadSingleSubscriptionBatchFromServer(contextPool, tcpStream, buffer);
                            try
                            {
                                await notifiedSubscribers;
                            }
                            catch (Exception)
                            {
                                incomingBatch.Item2.Dispose();
                                throw;
                            }
                            notifiedSubscribers = Task.Run(() =>
                            {
                                // ReSharper disable once AccessToDisposedClosure
                                using (incomingBatch.Item2)
                                {
                                    foreach (var curDoc in incomingBatch.Item1)
                                    {
                                        NotifySubscribers(curDoc.Data, out lastReceivedEtag);
                                    }
                                }
                                try
                                {
                                    if (tcpStream != null) //possibly prevent ObjectDisposedException
                                    {
                                        SendAck(lastReceivedEtag, tcpStream);
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

        private async Task<Tuple<List<SubscriptionConnectionServerMessage>, IDisposable>> ReadSingleSubscriptionBatchFromServer(JsonContextPool contextPool, Stream tcpStream, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            JsonOperationContext context;
            var incomingBatch = new List<SubscriptionConnectionServerMessage>();
            var returnContext = contextPool.AllocateOperationContext(out context);
            bool endOfBatch = false;
            while (endOfBatch == false && _proccessingCts.IsCancellationRequested == false)
            {
                var readObjectTask = ReadNextObject(context, tcpStream, buffer);

                var done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                if (done == _disposedTask.Task)
                {
                    break;
                }

                var receivedMessage = await readObjectTask.ConfigureAwait(false);
                if (_proccessingCts.IsCancellationRequested)
                    break;

                if (_proccessingCts.IsCancellationRequested)
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
                    case SubscriptionConnectionServerMessage.MessageType.Error:
                        switch (receivedMessage.Status)
                        {
                            case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                                throw new SubscriptionClosedException(receivedMessage.Exception ??
                                                                      string.Empty);
                            default:
                                throw new Exception(
                                    $"Connection terminated by server. Exception: {receivedMessage.Exception ?? "None"}");
                        }

                    default:
                        throw new ArgumentException(
                            $"Unrecognized message '{receivedMessage.Type}' type received from server");
                }
            }
            return Tuple.Create(incomingBatch, returnContext);
        }

        private async Task<SubscriptionConnectionServerMessage> ReadNextObject(JsonOperationContext context, Stream stream, JsonOperationContext.ManagedPinnedBuffer buffer)
        {
            if (_proccessingCts.IsCancellationRequested || _tcpClient.Connected == false)
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

        private void NotifySubscribers(BlittableJsonReaderObject curDoc, out long lastReceivedEtag)
        {
            if (curDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                ThrowMetadataRequired();
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
                ThrowIdRequired();
            if (metadata.TryGet(Constants.Documents.Metadata.Etag, out lastReceivedEtag) == false)
                ThrowEtagRequired();

            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"Got {id} (etag: {lastReceivedEtag} on subscription {_options.SubscriptionId}, size {curDoc.Size}");
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
                _proccessingCts.Token.ThrowIfCancellationRequested();
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

        private static void ThrowEtagRequired()
        {
            throw new InvalidOperationException("Document must have an ETag");
        }

        private static void ThrowIdRequired()
        {
            throw new InvalidOperationException("Document must have an id");
        }

        private static void ThrowMetadataRequired()
        {
            throw new InvalidOperationException("Document must have a metadata");
        }

        private void SendAck(long lastReceivedEtag, Stream networkStream)
        {
            BeforeAcknowledgment();

            var ack = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(new SubscriptionConnectionClientMessage
            {
                Etag = lastReceivedEtag,
                Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
            }));

            networkStream.Write(ack, 0, ack.Length);
            networkStream.Flush();
        }

        private async Task RunSubscriptionAsync(TaskCompletionSource<object> firstConnectionCompleted)
        {
            while (_proccessingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    CloseTcpClient();
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Subscription #{_options.SubscriptionId}. Connecting to server...");
                    }

                    _tcpClient = new TcpClient();

                    await ProccessSubscriptionAsync(firstConnectionCompleted).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (_proccessingCts.Token.IsCancellationRequested)
                    {
                        SubscriptionConnectionInterrupted(ex, true);
                        return;
                    }
                    firstConnectionCompleted.TrySetException(ex);
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(
                            $"Subscription #{_options.SubscriptionId}. Pulling task threw the following exception", ex);
                    }
                    if (await TryHandleRejectedConnectionOrDispose(ex, false).ConfigureAwait(false))
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Subscription #{_options.SubscriptionId}");
                        return;
                    }

                    await Task.Delay(_options.TimeToWaitBeforeConnectionRetryMilliseconds).ConfigureAwait(false);
                }
            }
            if (_proccessingCts.Token.IsCancellationRequested)
                return;

            if (IsErroredBecauseOfSubscriber)
            {
                try
                {
                    // prevent from calling Wait() on this in Dispose because we are already inside this task
                    await DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Subscription #{_options.SubscriptionId}. Exception happened during an attempt to close subscription after it had become faulted", e);
                    }
                }
            }
        }

        private async Task<bool> TryHandleRejectedConnectionOrDispose(Exception ex, bool reopenTried)
        {
            if (ex is SubscriptionInUseException || // another client has connected to the subscription
                ex is SubscriptionDoesNotExistException || // subscription has been deleted meanwhile
                ex is SubscriptionClosedException && reopenTried)
            // someone forced us to drop the connection by calling Subscriptions.Release
            {
                IsConnectionClosed = true;
                _taskCompletionSource.TrySetException(ex);
                SubscriptionConnectionInterrupted(ex, false);

                await DisposeAsync().ConfigureAwait(false);

                return true;
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
