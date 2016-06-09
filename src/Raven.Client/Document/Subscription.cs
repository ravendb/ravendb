// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Implementation;
using Raven.Client.Extensions;
using Raven.Client.Platform;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Json.Linq;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Client.Document
{
    public delegate void BeforeBatch();

    public delegate void AfterBatch(int documentsProcessed);

    public delegate bool BeforeAcknowledgment();

    public delegate void AfterAcknowledgment();

    // todo: find a way to use subscriptions in a way that will track and catch exceptions of the pulling proccess
    public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable where T : class
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Subscription<T>));
        private readonly AsyncManualResetEvent anySubscriber = new AsyncManualResetEvent();
        private readonly IAsyncDatabaseCommands commands;
        private readonly DocumentConvention conventions;
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        private readonly long id;
        private readonly bool isStronglyTyped;
        private readonly SubscriptionConnectionOptions _options;
        private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();
        private readonly RavenClientWebSocket webSocket;
        private bool completed;
        private IDisposable dataSubscriptionReleasedObserver;
        private bool disposed;
        private IDisposable endedBulkInsertsObserver;
        private bool firstConnection = true;
        private Task pullingTask;
        private Task startPullingTask;

        internal Subscription(long id, SubscriptionConnectionOptions options,
            IAsyncDatabaseCommands commands, DocumentConvention conventions)
        {
            this.id = id;
            _options = options;
            this.commands = commands;
            this.conventions = conventions;
            webSocket = new RavenClientWebSocket();

            if (typeof(T) != typeof(RavenJObject))
            {
                isStronglyTyped = true;
                generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                    entity => { throw new InvalidOperationException("Shouldn't be generating new ids here"); });
            }

            Start();
        }

        /// <summary>
        ///     It indicates if the subscription is in errored state because one of subscribers threw an exception.
        /// </summary>
        public bool IsErroredBecauseOfSubscriber { get; private set; }

        /// <summary>
        ///     The last exception thrown by one of subscribers.
        /// </summary>
        public Exception LastSubscriberException { get; private set; }

        /// <summary>
        ///     The last subscription connection exception.
        /// </summary>
        public Exception SubscriptionConnectionException { get; private set; }

        /// <summary>
        ///     It determines if the subscription connection is closed.
        /// </summary>
        public bool IsConnectionClosed { get; private set; }

        public void Dispose()
        {
            if (disposed)
                return;

            AsyncHelpers.RunSync(DisposeAsync);
        }

        public async Task DisposeAsync()
        {
            try
            {
                if (disposed)
                    return;

                disposed = true;

                OnCompletedNotification();

                subscribers.Clear();

                endedBulkInsertsObserver?.Dispose();

                dataSubscriptionReleasedObserver?.Dispose();

                cts.Cancel();

                anySubscriber.Set();

                foreach (var task in new[] { pullingTask, startPullingTask })
                {
                    if (task == null)
                        continue;

                    switch (task.Status)
                    {
                        case TaskStatus.RanToCompletion:
                        case TaskStatus.Canceled:
                            break;
                        default:
                            try
                            {
                                await task.ConfigureAwait(false);
                            }
                            catch (AggregateException ae)
                            {
                                if (ae.InnerException is OperationCanceledException == false &&
                                    ae.InnerException is WebSocketException == false)
                                {
                                    throw;
                                }
                            }
                            catch (WebSocketException)
                            {

                            }

                            break;
                    }
                }

                if (IsConnectionClosed)
                    return;

                await CloseSubscription().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // ignored
            }
            finally
            {
                try
                {
                    if (webSocket != null && webSocket.State == WebSocketState.Open)
                    {
                        var cancellationTokenSource = new CancellationTokenSource(1000);
                        await
                            webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure,
                                "{'Type': 'Closure', 'Message':'Subscription disposed on client side'}",
                                cancellationTokenSource.Token);
                    }
                }
                catch
                {
                    // ignored
                }
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (IsErroredBecauseOfSubscriber)
                throw new InvalidOperationException(
                    "Subscription encountered errors and stopped. Cannot add any subscriber.");

            if (subscribers.TryAdd(observer))
            {
                if (subscribers.Count == 1)
                    anySubscriber.Set();
            }

            return new DisposableAction(() =>
            {
                subscribers.TryRemove(observer);
                if (subscribers.Count == 0)
                    anySubscriber.Reset();
            });
        }

        public event BeforeBatch BeforeBatch = delegate { };
        public event AfterBatch AfterBatch = delegate { };
        public event BeforeAcknowledgment BeforeAcknowledgment = () => true; //TODO: what does it mean to return false here? Why would I do it?
        public event AfterAcknowledgment AfterAcknowledgment = delegate { };// TODO: what does this gives me that before/after batch don't?

        private void Start()
        {
            startPullingTask = StartPullingDocs();
        }

        private class WebSocketReadStream : Stream
        {
            private readonly RavenClientWebSocket _webSocket;
            private readonly CancellationToken _token;

            public WebSocketReadStream(RavenClientWebSocket webSocket, CancellationToken token)
            {
                _webSocket = webSocket;
                _token = token;
            }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                var receiveAsync = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer, offset, count), _token).ConfigureAwait(false);
                return receiveAsync.Count;
            }



            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => -1;
            public override long Position { get; set; }
        }

        private Task PullDocuments()
        {
            return Task.Run(async () =>
            {
                var queue = new BlockingCollection<RavenJObject>();
                var connectionCts = new CancellationTokenSource();
                var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(connectionCts.Token, cts.Token);
                try
                {
                    await anySubscriber.WaitAsync().ConfigureAwait(false);

                    var uri = new Uri(CreatePullingRequest().Url.Replace("http://", "ws://").Replace(".fiddler", ""));

                    using (var ms = new MemoryStream())
                    {
                        ms.SetLength(1024 * 4);
                        await webSocket.ConnectAsync(uri, cts.Token).ConfigureAwait(false);
                        cts.Token.ThrowIfCancellationRequested();

                        var firstRun = true;

                        using (
                            var reader = new StreamReader(new WebSocketReadStream(webSocket, cts.Token), Encoding.UTF8,
                                true, 1024, true))
                        using (var jsonReader = new JsonTextReaderAsync(reader))
                        {
                            cts.Token.ThrowIfCancellationRequested();
                            var connectionStatus =
                                (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);
                            AssertConnectionState(connectionStatus);
                            BeforeBatch();
                            while (cts.IsCancellationRequested == false)
                            {
                                cts.Token.ThrowIfCancellationRequested();
                                jsonReader.ResetState();
                                await jsonReader.ReadAsync().ConfigureAwait(false);

                                var curDoc =
                                    (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);

                                RavenJToken messageTypeToken;
                                if (curDoc.TryGetValue("Type", out messageTypeToken) == false)
                                    throw new ArgumentException($"Could not find message type field in data from server");

                                var messageType = curDoc["Type"].Value<string>();
                                if (messageType == "Data")
                                {
                                    queue.Add((RavenJObject)curDoc["Data"]);
                                    if (IsErroredBecauseOfSubscriber)
                                        break;

                                    if (firstRun)
                                    {
#pragma warning disable 4014
                                        Task.Factory.StartNew(async () =>
#pragma warning restore 4014
                                        {
                                            await ProcessDocs(queue, webSocket, linkedCts.Token).ConfigureAwait(false);
                                        }, TaskCreationOptions.LongRunning | TaskCreationOptions.AttachedToParent);

                                        firstRun = false;
                                    }
                                }
                                else if (messageType == "ConnectionState")
                                {
                                    connectionCts.Cancel();
                                    throw new SubscriptionClosedException("Connection terminated by server");
                                }

                                else
                                    throw new ArgumentException(
                                        $"Unrecognized message '{messageType}' type received from server");
                            }

                            cts.Token.ThrowIfCancellationRequested();
                        }
                    }
                }
                catch (OperationCanceledException cancelled)
                {
                    try
                    {
                        if (webSocket != null && webSocket.State == WebSocketState.Open)
                        {
                            using (var timedCancellationToken = new CancellationTokenSource(1000))
                            {
                                await
                                    webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure,
                                        "Connection terminated " + cancelled.Message, timedCancellationToken.Token);
                            }

                        }
                    }
                    catch
                    {
                        // ignored
                    }

                    throw;
                }
                catch (ArgumentException argument)
                {
                    try
                    {
                        if (webSocket != null && webSocket.State == WebSocketState.Open)
                        {
                            using (var timedCancellationToken = new CancellationTokenSource(1000))
                                await
                                    webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure,
                                    //TODO: send json here
                                        "Connection terminated by client, illegal argument: " + argument.Message, timedCancellationToken.Token);
                        }
                    }
                    catch
                    {
                        // ignored
                    }
                    throw;
                }
                finally
                {
                    queue.CompleteAdding();
                    connectionCts.Cancel();
                    cts.Token.ThrowIfCancellationRequested();
                }
            });
        }

        private void AssertConnectionState(RavenJObject connectionStatus)
        {
            RavenJToken typeToken;
            if (connectionStatus.TryGetValue("Type", out typeToken) == false)
                throw new ArgumentException("Type field was not received from server");
            if (typeToken.Value<string>() != "CoonectionStatus")
                throw new Exception("Server returned illegal status message");

            RavenJToken subscriptionStatusToken;
            if (connectionStatus.TryGetValue("Status", out subscriptionStatusToken) == false)
                throw new ArgumentException("Status field was not received from server");

            var subscriptionStatus = subscriptionStatusToken.Value<string>();

            switch (subscriptionStatus)
            {
                case "Accepted":
                    break;
                case "InUse":
                    throw new SubscriptionInUseException(
                        $"Subscription With Id {this.id} cannot be opened, because it's in use and the connection strategy is {this._options.Strategy}");
                case "Closed":
                    throw new SubscriptionClosedException(
                        $"Subscription With Id {this.id} cannot be opened, because it was closed");
                case "NotFound":
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id {this.id} cannot be opened, because it does not exist");
                default:
                    throw new ArgumentException(
                        $"Subscription {this.id} could not be opened, reason: {subscriptionStatus}");
            }
        }

        private async Task ProcessDocs(BlockingCollection<RavenJObject> queue, RavenClientWebSocket ws, CancellationToken ct)
        {
            var proccessedDocsInCurrentBatch = 0;
            long lastReceivedEtag = 0;


            while (ct.IsCancellationRequested == false)
            {
                RavenJObject doc;
                T instance;
                if (queue.TryTake(out doc) == false)
                {
                    // This is an acknowledge when the server returns documents to the subscriber.
                    if (BeforeAcknowledgment())
                    {
                        await AcknowledgeBatchToServer(webSocket, cts.Token, lastReceivedEtag).ConfigureAwait(false);
                        AfterAcknowledgment();
                    }

                    AfterBatch(proccessedDocsInCurrentBatch);
                    proccessedDocsInCurrentBatch = 0;
                    if (queue.TryTake(out doc, Timeout.Infinite) == false)
                        break;
                    BeforeBatch();
                }

                proccessedDocsInCurrentBatch++;
                var metadata = doc["@metadata"] as RavenJObject;

                // ReSharper disable once PossibleNullReferenceException
                lastReceivedEtag = metadata["@etag"].Value<long>();

                if (isStronglyTyped)
                {
                    instance = doc.Deserialize<T>(conventions);

                    var docId = doc[Constants.Metadata].Value<string>("@id");

                    if (string.IsNullOrEmpty(docId) == false)
                        generateEntityIdOnTheClient.TrySetIdentity(instance, docId);
                }
                else
                {
                    instance = (T)(object)doc;
                }

                foreach (var subscriber in subscribers)
                {
                    if (cts.IsCancellationRequested)
                        break;
                    try
                    {
                        subscriber.OnNext(instance);
                    }
                    catch (Exception ex)
                    {
                        logger.WarnException(
                            string.Format(
                                "Subscription #{0}. Subscriber threw an exception", id), ex);

                        if (_options.IgnoreSubscribersErrors == false)
                        {
                            IsErroredBecauseOfSubscriber = true;
                            LastSubscriberException = ex;

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

                if (IsErroredBecauseOfSubscriber)
                    break;

            }
        }

        private async Task AcknowledgeBatchToServer(RavenClientWebSocket ws, CancellationToken ct, long lastReceivedEtag)
        {
            using (var ms = new MemoryStream())
            {
                var ackJson = new RavenJObject
                {
                    ["Type"] = "ACK",
                    ["Data"] = lastReceivedEtag
                };

                ackJson.WriteTo(ms);

                ArraySegment<byte> buffer;

                ms.TryGetBuffer(out buffer);
                await ws.SendAsync(buffer, WebSocketMessageType.Text, true, ct).ConfigureAwait(false);
            }
        }

        private async Task StartPullingDocs()
        {
            SubscriptionConnectionException = null;

            pullingTask = PullDocuments().ObserveException();

            try
            {
                await pullingTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (cts.Token.IsCancellationRequested)
                    return;

                logger.WarnException(
                    string.Format("Subscription #{0}. Pulling task threw the following exception", id), ex);


                // todo: implement handling of rejected connection
                if (TryHandleRejectedConnection(ex, false))
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug(string.Format("Subscription #{0}. Stopping the connection '{1}'", id,
                            _options.ConnectionId));
                    return;
                }

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                RestartPullingTask().ConfigureAwait(false);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            }

            if (IsErroredBecauseOfSubscriber)
            {
                try
                {
                    startPullingTask = null;
                    // prevent from calling Wait() on this in Dispose because we are already inside this task
                    await DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Exception happened during an attempt to close subscription after it had become faulted",
                            id), e);
                }
            }
        }

        private async Task RestartPullingTask()
        {
            await Time.Delay(_options.TimeToWaitBeforeConnectionRetryTimespan).ConfigureAwait(false);
            startPullingTask = StartPullingDocs().ObserveException();
        }

        private bool TryHandleRejectedConnection(Exception ex, bool reopenTried)
        {
            SubscriptionConnectionException = ex;

            if (ex is SubscriptionInUseException || // another client has connected to the subscription
                ex is SubscriptionDoesNotExistException || // subscription has been deleted meanwhile
                (ex is SubscriptionClosedException && reopenTried))
            // someone forced us to drop the connection by calling Subscriptions.Release
            {
                IsConnectionClosed = true;

                startPullingTask = null;
                // prevent from calling Wait() on this in Dispose because we can be already inside this task
                pullingTask = null;
                // prevent from calling Wait() on this in Dispose because we can be already inside this task

                Dispose();

                return true;
            }

            return false;
        }

        private HttpJsonRequest CreatePullingRequest()
        {
            return
                commands.CreateRequest(
                    $"/subscriptions/pull?id={id}&connection={_options.ConnectionId}" +
                    $"&strategy={_options.Strategy}&maxDocsPerBatch={_options.MaxDocsPerBatch}" +
                    (_options.MaxBatchSize.HasValue ? "&maxBatchSize" + _options.MaxBatchSize.Value : ""),
                    HttpMethod.Get);
        }

        private HttpJsonRequest CreateCloseRequest()
        {
            return
                commands.CreateRequest(
                    string.Format("/subscriptions/close?id={0}&connection={1}", id, _options.ConnectionId),
                    HttpMethods.Post);
        }

        private void OnCompletedNotification()
        {
            if (completed)
                return;

            foreach (var subscriber in subscribers)
            {
                subscriber.OnCompleted();
            }

            completed = true;
        }

        private async Task CloseSubscription()
        {
            using (var closeRequest = CreateCloseRequest())
            {
                await closeRequest.ExecuteRequestAsync().ConfigureAwait(false);
                IsConnectionClosed = true;
            }
        }
    }
}
