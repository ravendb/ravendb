// -----------------------------------------------------------------------
//  <copyright file="Subscription.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    public delegate void BeforeBatch();

    public delegate void AfterBatch(int documentsProcessed);

    public delegate void BeforeAcknowledgment();

    public delegate void AfterAcknowledgment();

    public class Subscription<T> : IObservable<T>, IDisposableAsync, IDisposable where T : class
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(Subscription<T>));
        private readonly AsyncServerClient _commands;
        private readonly DocumentConvention _conventions;
        private readonly CancellationTokenSource _proccessingCts = new CancellationTokenSource();
        private readonly GenerateEntityIdOnTheClient _generateEntityIdOnTheClient;
        private readonly bool _isStronglyTyped;
        private readonly SubscriptionConnectionOptions _options;
        private readonly List<IObserver<T>> _subscribers = new List<IObserver<T>>();
        private TcpClient _tcpClient;
        private bool _completed, _started;
        private bool _disposed;
        private Task _subscriptionTask;

        internal Subscription(SubscriptionConnectionOptions options,
            AsyncServerClient commands, DocumentConvention conventions)
        {
            _options = options;
            if (_options.SubscriptionId == 0)
                throw new ArgumentException("SubscriptionConnectionOptions must specify the SubscriptionId, but was set to zero.",
                    nameof(options));
            _commands = commands;
            _conventions = conventions;

            if (typeof(T) != typeof(RavenJObject))
            {
                _isStronglyTyped = true;
                _generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(conventions,
                    entity => { throw new InvalidOperationException("Shouldn't be generating new ids here"); });
            }
        }

        ~Subscription()
        {
            if (_disposed) return;
            try
            {
                CloseTcpClient();
                Logger.Warn($"Subscription {_options.SubscriptionId} was not disposed properly");
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
                _proccessingCts.Cancel();
                CloseTcpClient(); // we disconnect immediately, freeing the subscription task
                if (_subscriptionTask != null && Task.CurrentId != _subscriptionTask.Id)
                {
                    try
                    {
                        await _subscriptionTask;
                    }
                    catch (Exception)
                    {
                        // just need to wait for it to end
                    }
                }
                OnCompletedNotification();
            }
            catch (Exception ex)
            {
                Logger.Warn("Error during dispose of subscription", ex);
            }
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (_started)
                throw new InvalidOperationException(
                    "You can only add observers to a subscriptions before you started it");

            if (IsErroredBecauseOfSubscriber)
                throw new InvalidOperationException(
                    "Subscription encountered errors and stopped. Cannot add any subscriber.");
            _subscribers.Add(observer);

            // we cannot remove subscriptions dynamically, once we added, it is done
            return new DisposableAction(() => { });
        }

        public event BeforeBatch BeforeBatch = delegate { };
        public event AfterBatch AfterBatch = delegate { };
        public event BeforeAcknowledgment BeforeAcknowledgment = delegate { };
        /// <summary>
        /// allows the user to define stuff that happens after the confirm was recieved from the server (this way we know we won't
        /// get those documents again)
        /// </summary>
        public event AfterAcknowledgment AfterAcknowledgment = delegate { };

        public void Start()
        {
            if (_started)
                return;

            if (_subscribers.Count == 0)
                throw new InvalidOperationException(
                    "No observers has been registered, did you forget to call Subscribe?");
            _started = true;
            _subscriptionTask = Task.Run(RunSubscriptionAsync);
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

            buffer = Encoding.UTF8.GetBytes(RavenJObject.FromObject(_options).ToString());
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
                    Logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Subscriber threw an exception while proccessing OnError", _options.SubscriptionId), ex);
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
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it's in use and the connection strategy is {this._options.Strategy}");
                case "Closed":
                    throw new SubscriptionClosedException(
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it was closed");
                case "NotFound":
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it does not exist");
                default:
                    throw new ArgumentException(
                        $"Subscription {this._options.SubscriptionId} could not be opened, reason: {subscriptionStatus}");
            }
        }

        private async Task ProccessSubscription()
        {
            bool succesffullyConnectedToServer = false;
            try
            {
                _proccessingCts.Token.ThrowIfCancellationRequested();

                using (var tcpStream = await ConnectToServer().ConfigureAwait(false))
                using (var reader = new StreamReader(tcpStream))
                using (var jsonReader = new JsonTextReaderAsync(reader))
                {
                    _proccessingCts.Token.ThrowIfCancellationRequested();
                    var connectionStatus = (RavenJObject)await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false);
                    AssertConnectionState(connectionStatus);
                    succesffullyConnectedToServer = true;
                    var readObjectTask = ReadNextObject(jsonReader);

                    var incomingBatch = new List<RavenJObject>();
                    long lastReceivedEtag = 0;


                    while (_proccessingCts.IsCancellationRequested == false)
                    {
                        BeforeBatch();
                        while (_proccessingCts.IsCancellationRequested == false)
                        {
                            var receivedMessage = (RavenJObject)await readObjectTask.ConfigureAwait(false);
                            if (_proccessingCts.IsCancellationRequested)
                                return;

                            var messageType = AssertAndReturnReceivedMessageType(receivedMessage);

                            readObjectTask = ReadNextObject(jsonReader);

                            if (messageType == "EndOfBatch")
                                break;

                            if (messageType == "Confirm")
                            {
                                AfterAcknowledgment();
                                AfterBatch(incomingBatch.Count);
                                incomingBatch.Clear();
                            }
                            else
                            {
                                incomingBatch.Add((RavenJObject)receivedMessage["Data"]);
                            }
                        }

                        foreach (var curDoc in incomingBatch)
                        {
                            NotifySubscribers(curDoc, out lastReceivedEtag);
                        }

                        SendAck(lastReceivedEtag, tcpStream);
                    }
                }
            }
            catch (Exception ex)
            {
                InformSubscribersOnError(ex);
                try
                {
                    ex.Data["SuccesffullyConnectedToServer"] = succesffullyConnectedToServer;
                }
                catch (Exception)
                {
                }
                throw;
            }
            finally
            {
                _proccessingCts.Token.ThrowIfCancellationRequested();
            }
        }

        private async Task<RavenJToken> ReadNextObject(JsonTextReaderAsync jsonReader)
        {
            do
            {
                if (_proccessingCts.IsCancellationRequested)
                    return null;
                jsonReader.ResetState();
            } while (await jsonReader.ReadAsync() == false);// need to do that to handle the heartbeat whitespace 
            return await RavenJObject.LoadAsync(jsonReader);
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

        private void NotifySubscribers(RavenJObject curDoc, out long lastReceivedEtag)
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

            foreach (var subscriber in _subscribers)
            {
                _proccessingCts.Token.ThrowIfCancellationRequested();
                try
                {
                    subscriber.OnNext(instance);
                }
                catch (Exception ex)
                {
                    Logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Subscriber threw an exception", _options.SubscriptionId), ex);

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
            BeforeAcknowledgment();
            var ackJson = new RavenJObject
            {
                ["Type"] = "Acknowledge",
                ["Etag"] = lastReceivedEtag
            };

            ackJson.WriteTo(networkStream);
        }

        private async Task RunSubscriptionAsync()
        {
            Exception subscriptionConnectionException = null;

            int retries = 15;

            while (retries-- > 0 && _proccessingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    CloseTcpClient();
                    Logger.Debug(string.Format("Subscription #{0}. Connection to server...", _options.SubscriptionId));

                    _tcpClient = new TcpClient();
                    await ProccessSubscription();
                }
                catch (Exception ex)
                {
                    subscriptionConnectionException = ex;
                    if (_proccessingCts.Token.IsCancellationRequested)
                        return;

                    if ((bool?)ex.Data["SuccesffullyConnectedToServer"] == true)
                    {
                        retries = 15; // we connected to the server, so reset the retries
                    }

                    Logger.WarnException(
                        string.Format("Subscription #{0}. Pulling task threw the following exception", _options.SubscriptionId), ex);


                    if (await TryHandleRejectedConnection(ex, false).ConfigureAwait(false))
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug(string.Format("Subscription #{0}.", _options.SubscriptionId));
                        InformSubscribersOnError(ex);
                        return;
                    }
                    if (retries > 0)
                        await Task.Delay(_options.TimeToWaitBeforeConnectionRetryMilliseconds);
                }
            }
            if (_proccessingCts.Token.IsCancellationRequested)
                return;
            if (subscriptionConnectionException != null)
                InformSubscribersOnError(subscriptionConnectionException);

            if (IsErroredBecauseOfSubscriber)
            {
                try
                {
                    // prevent from calling Wait() on this in Dispose because we are already inside this task
                    await DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.WarnException(
                        string.Format(
                            "Subscription #{0}. Exception happened during an attempt to close subscription after it had become faulted",
                            _options.SubscriptionId), e);
                }
            }
        }

        private async Task<bool> TryHandleRejectedConnection(Exception ex, bool reopenTried)
        {
            if (ex is SubscriptionInUseException || // another client has connected to the subscription
                ex is SubscriptionDoesNotExistException || // subscription has been deleted meanwhile
                (ex is SubscriptionClosedException && reopenTried))
            // someone forced us to drop the connection by calling Subscriptions.Release
            {
                IsConnectionClosed = true;

                InformSubscribersOnError(ex);

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
