// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
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
        private readonly AsyncManualResetEvent _anySubscriber = new AsyncManualResetEvent();
        private readonly AsyncServerClient _commands;
        private readonly DocumentConvention _conventions;
        private readonly CancellationTokenSource _proccessingCts = new CancellationTokenSource();
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;
        private readonly long _id;
        private readonly bool _isStronglyTyped;
        private readonly SubscriptionConnectionOptions _options;
        private readonly ConcurrentSet<IObserver<T>> _subscribers = new ConcurrentSet<IObserver<T>>();
        private TcpClient _tcpClient;
        private bool _completed;
        private bool _disposed;
        private Task _subscriptionProccessTask;
        private Task _startSubscriptionProccessTask;

        internal Subscription(long id, SubscriptionConnectionOptions options,
            AsyncServerClient commands, DocumentConvention conventions)
        {
            _id = id;
            _options = options;
            _commands = commands;
            _conventions = conventions;
            _tcpClient = new TcpClient();

            if (typeof(T) != typeof(RavenJObject))
            {
                _isStronglyTyped = true;
                _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                    entity => { throw new InvalidOperationException("Shouldn't be generating new ids here"); });
            }

            Start();
        }

        ~Subscription()
        {
            if (_disposed) return;
            try
            {
                _tcpClient.Dispose();
                logger.Warn($"Subscription {_id} was not disposed properly");
                //write to log
            }
            catch
            {
                    
            }
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
                OnCompletedNotification();
                _subscribers.Clear();
                _proccessingCts.Cancel();
                _anySubscriber.Set();

                foreach (var task in new[] { _subscriptionProccessTask, _startSubscriptionProccessTask })
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
                            catch (Exception ex)
                            {
                            }
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                // log that
            }
            finally
            {
                CloseTcpClient();
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (IsErroredBecauseOfSubscriber)
                throw new InvalidOperationException(
                    "Subscription encountered errors and stopped. Cannot add any subscriber.");

            if (_subscribers.TryAdd(observer))
            {
                if (_subscribers.Count == 1)
                    _anySubscriber.Set();
            }

            return new DisposableAction(() =>
            {
                _subscribers.TryRemove(observer);
                if (_subscribers.Count == 0)
                    _anySubscriber.Reset();
            });
        }

        public event BeforeBatch BeforeBatch = delegate { };
        public event AfterBatch AfterBatch = delegate { };
        public event BeforeAcknowledgment BeforeAcknowledgment = () => true; //TODO: what does it mean to return false here? Why would I do it?
        public event AfterAcknowledgment AfterAcknowledgment = delegate { };// TODO: what does this gives me that before/after batch don't?

        private void Start()
        {
            _startSubscriptionProccessTask = StartSubscriptionProccessTask();
        }

        private async Task<Stream> ConnectToServer()
        {
            var connectionInfo = await _commands.GetTcpInfoAsync();
            await _tcpClient.ConnectAsync(new Uri(connectionInfo.Url).Host, connectionInfo.Port);

            _tcpClient.NoDelay = true;
            _tcpClient.SendBufferSize = 32 * 1024;
            _tcpClient.ReceiveBufferSize = 4096;
            var networkStream = _tcpClient.GetStream();

            var buffer = Encoding.UTF8.GetBytes(new RavenJObject
            {
                ["Database"] = MultiDatabase.GetDatabaseName(_commands.Url),
                ["Operation"] = "Subscription"
            }.ToString());
            await networkStream.WriteAsync(buffer, 0, buffer.Length);

            return networkStream;
        }

        private void InformSubscribersOnError(Exception ex)
        {
            foreach (var subscriber in _subscribers)
            {
                try
                {
                    subscriber.OnError(ex);
                }
                catch (Exception e)
                {
                    logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Subscriber threw an exception while proccessing OnError", _id), ex);
                }
            }
        }

        private void AssertConnectionState(RavenJObject connectionStatus)
        {
            RavenJToken typeToken;
            if (connectionStatus.TryGetValue("Type", out typeToken) == false)
                throw new ArgumentException("Type field was not received from server");
            var messageType = typeToken.Value<string>();
            if (messageType != "CoonectionStatus")
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
                        $"Subscription With Id {this._id} cannot be opened, because it's in use and the connection strategy is {this._options.Strategy}");
                case "Closed":
                    throw new SubscriptionClosedException(
                        $"Subscription With Id {this._id} cannot be opened, because it was closed");
                case "NotFound":
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id {this._id} cannot be opened, because it does not exist");
                default:
                    throw new ArgumentException(
                        $"Subscription {this._id} could not be opened, reason: {subscriptionStatus}");
            }
        }
        
        private Task ProccessSubscription()
        {
            return Task.Run(async () =>
            {
                try
                {
                    await _anySubscriber.WaitAsync().ConfigureAwait(false);
                    _proccessingCts.Token.ThrowIfCancellationRequested();

                    using (var tcpStream = await ConnectToServer())
                    using (var reader = new StreamReader(tcpStream, Encoding.UTF8,true, 1024, true))
                    using (var jsonReader = new JsonTextReaderAsync(reader))
                    {
                        try
                        {
                            _proccessingCts.Token.ThrowIfCancellationRequested();
                            var connectionStatus = (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);
                            AssertConnectionState(connectionStatus);
                            long lastReceivedEtag = 0;
                            var incomingBatch = new List<RavenJObject>();
                            jsonReader.ResetState();
                            var readingTask = jsonReader.ReadAsync();

                            // todo: we want have one batch that we proccess and one batch that we load while proccessing..
                            while (_proccessingCts.IsCancellationRequested == false)
                            {
                                BeforeBatch();
                                while (_proccessingCts.IsCancellationRequested == false)
                                {
                                    _proccessingCts.Token.ThrowIfCancellationRequested();
                                    await readingTask.ConfigureAwait(false);

                                    var receivedMessage = (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);

                                    var messageType = AssertAndReturnReceivedMessageType(receivedMessage);
                                    readingTask = jsonReader.ReadAsync();

                                    if (messageType == "EndOfBatch")
                                        break;

                                    if (messageType == "Confirm")
                                    {
                                        // move to while above
                                        AfterAcknowledgment();
                                        AfterBatch(incomingBatch.Count);
                                        incomingBatch.Clear();

                                    }
                                    else
                                    {
                                        incomingBatch.Add((RavenJObject)receivedMessage["Data"]);
                                    }

                                    
                                    // reset json reader state machine
                                    jsonReader.ResetState();
                                }

                                foreach (var curDoc in incomingBatch)
                                {
                                    NotifySubscribers(curDoc, readingTask, out lastReceivedEtag);
                                }

                                SendAck(lastReceivedEtag, tcpStream);
                            }
                        }
                        finally
                        {
                            try
                            {
                                SendSubscriptionTermination(tcpStream);
                            }
                            catch
                            {

                            }
                        }
                    }
                    
                }
                catch (Exception ex)
                {
                    InformSubscribersOnError(ex);
                    throw;
                }
                finally
                {
                    _proccessingCts.Token.ThrowIfCancellationRequested();
                }
            });
        }

        private static string AssertAndReturnReceivedMessageType(RavenJObject receivedMessage)
        {
            RavenJToken messageTypeToken;
            if (receivedMessage.TryGetValue("Type", out messageTypeToken) == false)
                throw new ArgumentException(
                    $"Could not find message type field in data from server");

            var messageType = receivedMessage["Type"].Value<string>();
            if (messageType == "Data" || messageType == "EndOfBatch" || messageType == "Confirm")
            {
                return messageType;
            }

            if (messageType == "Terminated")
            {
                throw new SubscriptionClosedException("Connection terminated by server");
            }
            
            throw new ArgumentException(
                $"Unrecognized message '{messageType}' type received from server");
        }

        private void NotifySubscribers(RavenJObject curDoc, Task<bool> readingTask,out long lastReceivedEtag)
        {
            T instance;
            var metadata = curDoc[Constants.Metadata] as RavenJObject;
            lastReceivedEtag = metadata[Constants.MetadataEtagId].Value<long>();

            if (_isStronglyTyped)
            {
                instance = curDoc.Deserialize<T>(_conventions);

                var docId = metadata[Constants.MetadataDocId].Value<string>();

                if (string.IsNullOrEmpty(docId) == false)
                    _generateEntityIdOnTheClient.TrySetIdentity(instance, docId);
            }
            else
            {
                instance = (T)(object)curDoc;
            }

            // todo: consider parallelize this..
            foreach (var subscriber in _subscribers)
            {
                if (_proccessingCts.IsCancellationRequested || readingTask.IsCompleted)
                    break;
                try
                {
                    subscriber.OnNext(instance);
                }
                catch (Exception ex)
                {
                    logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Subscriber threw an exception", _id), ex);

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
        }

        private void SendAck(long lastReceivedEtag, Stream networkStream)
        {
            if (!BeforeAcknowledgment()) return;

            var ackJson = new RavenJObject
            {
                ["Type"] = "Acknowledge",
                ["Data"] = lastReceivedEtag
            };

            ackJson.WriteTo(networkStream);
        }

        private void SendSubscriptionTermination(Stream networkStream)
        {
            if (!BeforeAcknowledgment()) return;

            var ackJson = new RavenJObject
            {
                ["Type"] = "Terminated"
            };

            ackJson.WriteTo(networkStream);

            AfterAcknowledgment();
        }

        private async Task StartSubscriptionProccessTask()
        {
            SubscriptionConnectionException = null;

            _subscriptionProccessTask = ProccessSubscription().ObserveException();

            try
            {
                await _subscriptionProccessTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (_proccessingCts.Token.IsCancellationRequested)
                    return;

                logger.WarnException(
                    string.Format("Subscription #{0}. Pulling task threw the following exception", _id), ex);


                // todo: implement handling of rejected connection
                if (await TryHandleRejectedConnection(ex, false).ConfigureAwait(false))
                {
                    if (logger.IsDebugEnabled)
                        logger.Debug(string.Format("Subscription #{0}. Stopping the connection '{1}'", _id,
                            _options.ConnectionId));
                    return;
                }

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                RestartPullingTask();
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            }

            if (IsErroredBecauseOfSubscriber)
            {
                try
                {
                    _startSubscriptionProccessTask = null;
                    // prevent from calling Wait() on this in Dispose because we are already inside this task
                    await DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Exception happened during an attempt to close subscription after it had become faulted",
                            _id), e);
                }
            }
        }

        private void RestartPullingTask()
        {
            CloseTcpClient();
            _tcpClient = new TcpClient();
            _startSubscriptionProccessTask = StartSubscriptionProccessTask().ObserveException();
        }

        private async Task<bool> TryHandleRejectedConnection(Exception ex, bool reopenTried)
        {
            SubscriptionConnectionException = ex;

            if (ex is SubscriptionInUseException || // another client has connected to the subscription
                ex is SubscriptionDoesNotExistException || // subscription has been deleted meanwhile
                (ex is SubscriptionClosedException && reopenTried))
            // someone forced us to drop the connection by calling Subscriptions.Release
            {
                IsConnectionClosed = true;

                _startSubscriptionProccessTask = null;
                // prevent from calling Wait() on this in Dispose because we can be already inside this task
                _subscriptionProccessTask = null;
                // prevent from calling Wait() on this in Dispose because we can be already inside this task

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
                subscriber.OnCompleted();
            }

            _completed = true;
        }

        
        private void CloseTcpClient()
        {
            if (_tcpClient != null)
            {
                try
                {
                    _tcpClient.Dispose();
                }
                catch
                {
                    
                }
            }
        }
    }
}
