// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
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
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Client.Document
{
    public delegate void BeforeBatch();

    public delegate void AfterBatch(int documentsProcessed);

    public delegate bool BeforeAcknowledgment();

    public delegate void AfterAcknowledgment();

    public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable,
        IObserver<DataSubscriptionChangeNotification> where T : class
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(Subscription<T>));
        private readonly ManualResetEvent anySubscriber = new ManualResetEvent(false);
        private readonly IAsyncDatabaseCommands commands;
        private readonly DocumentConvention conventions;
        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly Func<Task> ensureOpenSubscription;
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        private readonly long id;
        private readonly bool isStronglyTyped;
        private readonly SubscriptionConnectionOptions options;
        private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();
        private readonly RavenClientWebSocket webSocket;
        private bool completed;
        private IDisposable dataSubscriptionReleasedObserver;
        private bool disposed;
        private IDisposable endedBulkInsertsObserver;
        private bool firstConnection = true;
        private Task pullingTask;
        private Task startPullingTask;

        internal Subscription(long id, string database, SubscriptionConnectionOptions options,
            IAsyncDatabaseCommands commands, DocumentConvention conventions, bool open,
            Func<Task> ensureOpenSubscription)
        {
            this.id = id;
            this.options = options;
            this.commands = commands;
            this.conventions = conventions;
            this.ensureOpenSubscription = ensureOpenSubscription;
            webSocket = new RavenClientWebSocket();

            if (typeof(T) != typeof(RavenJObject))
            {
                isStronglyTyped = true;
                generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                    entity =>
                        AsyncHelpers.RunSync(() => conventions.GenerateDocumentKeyAsync(database, commands, entity)));
            }

            if (open)
                Start();
            else
            {
                if (options.Strategy != SubscriptionOpeningStrategy.WaitForFree)
                    throw new InvalidOperationException("Subscription isn't open while its opening strategy is: " +
                                                        options.Strategy);
            }

            if (options.Strategy == SubscriptionOpeningStrategy.WaitForFree)
                WaitForSubscriptionReleased();
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

            DisposeAsync().Wait();
        }

        public Task DisposeAsync()
        {
            if (disposed)
                return new CompletedTask();

            disposed = true;

            OnCompletedNotification();

            subscribers.Clear();

            if (endedBulkInsertsObserver != null)
                endedBulkInsertsObserver.Dispose();

            if (dataSubscriptionReleasedObserver != null)
                dataSubscriptionReleasedObserver.Dispose();

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
                            task.Wait();
                        }
                        catch (AggregateException ae)
                        {
                            if (ae.InnerException is OperationCanceledException == false &&
                                ae.InnerException is WebSocketException == false)
                            {
                                throw;
                            }
                        }

                        break;
                }
            }

            if (IsConnectionClosed)
                return new CompletedTask();

            return CloseSubscription();
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

        // todo: make sure to take care of treating subscription changes
        public void OnNext(DataSubscriptionChangeNotification notification)
        {
            if (notification.Type != DataSubscriptionChangeTypes.SubscriptionReleased)
                return;
            try
            {
                ensureOpenSubscription().Wait();
            }
            catch (Exception)
            {
                return;
            }

            // succeeded in opening the subscription

            // no longer need to be notified about subscription status changes
            dataSubscriptionReleasedObserver.Dispose();
            dataSubscriptionReleasedObserver = null;

            // start standard stuff
            Start();
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public event BeforeBatch BeforeBatch = delegate { };
        public event AfterBatch AfterBatch = delegate { };
        public event BeforeAcknowledgment BeforeAcknowledgment = () => true;
        public event AfterAcknowledgment AfterAcknowledgment = delegate { };

        private void Start()
        {
            startPullingTask = StartPullingDocs();
        }

        private class WebSocketReadStream : Stream
        {
            private readonly RavenClientWebSocket _webSocket;
            private readonly CancellationToken _token;

            public WebSocketReceiveResult LastResult;

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
                var receiveAsync = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer, offset, count), _token);
                LastResult = receiveAsync;
                return receiveAsync.Count;
            }



            public override bool CanRead => true;
            public override bool CanSeek { get; }
            public override bool CanWrite { get; }
            public override long Length { get; }
            public override long Position { get; set; }
        }





        private Task PullDocuments()
        {
            return Task.Run(async () =>
            {
                var queue = new BlockingCollection<RavenJObject>();
                try
                {
                    var uri = new Uri(CreatePullingRequest().Url.Replace("http://", "ws://").Replace(".fiddler", ""));

                    using (var ms = new MemoryStream())
                    {
                        ms.SetLength(1024 * 4);
                        await webSocket.ConnectAsync(uri, cts.Token).ConfigureAwait(false);
                        // this is terrible, remove this, implement AsyncServerClient.YieldStreamResults for websockets

                        anySubscriber.WaitOne();

                        cts.Token.ThrowIfCancellationRequested();


                        var processingTask = Task.CompletedTask;
                        var firstRun = true;

                        cts.Token.ThrowIfCancellationRequested();
                        using (
                            var reader = new StreamReader(new WebSocketReadStream(webSocket, cts.Token), Encoding.UTF8,
                                true, 1024, true))
                        using (var jsonReader = new JsonTextReaderAsync(reader))
                        {
                            while (cts.IsCancellationRequested == false)
                            {
                                jsonReader.ResetState();
                                await jsonReader.ReadAsync().ConfigureAwait(false);

                                var curDoc =
                                    (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);
                                queue.Add(curDoc);
                                if (IsErroredBecauseOfSubscriber)
                                    break;

                                if (firstRun)
                                {
                                    processingTask = Task.Run(() =>
                                    {
#pragma warning disable 4014
                                        ProcessDocs(queue, webSocket, cts.Token);
#pragma warning restore 4014
                                    });
                                    firstRun = false;
                                }
                            }
                        }
                        queue.CompleteAdding();

                        await processingTask;
                        return Task.CompletedTask;
                    }
                }
                catch (ErrorResponseException e)
                {
                    queue.CompleteAdding();
                    cts.Cancel();
                    SubscriptionException subscriptionException;
                    if (AsyncDocumentSubscriptions.TryGetSubscriptionException(e, out subscriptionException))
                        throw subscriptionException;

                    throw;
                }
            });
        }

        private async Task ProcessDocs(BlockingCollection<RavenJObject> queue, RavenClientWebSocket ws, CancellationToken ct)
        {
            var proccessedDocsInCurrentBatch = 0;
            long lastReceivedEtag = 0;
            BeforeBatch();
            using (var ms = new MemoryStream())
                while (ct.IsCancellationRequested == false)
                {
                    RavenJObject doc;
                    T instance;
                    if (queue.TryTake(out doc) == false)
                    {
                        // This is an acknowledge when the server returns documents to the subscriber.
                        if (BeforeAcknowledgment())
                        {
                            await AcknowledgeBatchToServer(webSocket, cts.Token, lastReceivedEtag);

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
                        try
                        {
                            subscriber.OnNext(instance);
                        }
                        catch (Exception ex)
                        {
                            logger.WarnException(
                                string.Format(
                                    "Subscription #{0}. Subscriber threw an exception", id), ex);

                            if (options.IgnoreSubscribersErrors == false)
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
                    ["LastEtag"] = lastReceivedEtag
                };

                ackJson.WriteTo(ms);

                ArraySegment<byte> buffer;

                ms.TryGetBuffer(out buffer);
                await ws.SendAsync(buffer, WebSocketMessageType.Text, true, ct);
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

                if (TryHandleRejectedConnection(ex, false))
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug(string.Format("Subscription #{0}. Stopping the connection '{1}'", id,
                            options.ConnectionId));
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
            await Time.Delay(options.TimeToWaitBeforeConnectionRetryTimespan).ConfigureAwait(false);
            try
            {
                await ensureOpenSubscription().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (TryHandleRejectedConnection(ex, true))
                    return;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                RestartPullingTask().ConfigureAwait(false);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                return;
            }

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


        // todo: make sure we take care of that, through changes?
        private void WaitForSubscriptionReleased()
        {
            /*var dataSubscriptionObservable = changes.ForDataSubscription(id);

            dataSubscriptionReleasedObserver = dataSubscriptionObservable.Subscribe(this);

            dataSubscriptionObservable.Task.Wait();*/
        }

        private HttpJsonRequest CreatePullingRequest()
        {
            return
                commands.CreateRequest(
                    string.Format("/subscriptions/pull?id={0}&connection={1}", id, options.ConnectionId), HttpMethod.Get,
                    timeout: options.PullingRequestTimeoutTimespan);
        }

        private HttpJsonRequest CreateCloseRequest()
        {
            return
                commands.CreateRequest(
                    string.Format("/subscriptions/close?id={0}&connection={1}", id, options.ConnectionId),
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