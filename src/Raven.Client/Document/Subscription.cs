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
        private Task<int> _subscriptionTask;
        private NetworkStream _networkStream;
        private readonly TaskCompletionSource<object> _disposedTask = new TaskCompletionSource<object>();

        private static long Counter;

        public long _localId = Interlocked.Increment(ref Counter);
        private long _lastReceivedEtag;

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
            Console.WriteLine($"[{_localId}] :: Enter dispose");
            Console.Out.Flush();
            try
            {
                if (_disposed)
                    return;

                _disposed = true;
                _proccessingCts.Cancel();
                _disposedTask.TrySetResult(null); // notify the subscription task that we are done

                if (_subscriptionTask != null && Task.CurrentId != _subscriptionTask.Id)
                {
                    try
                    {
                        Console.WriteLine($"[{_localId}] :: awaiting _subscriptionTask.Id={_subscriptionTask.Id}");
                        Console.Out.Flush();

                        var i = await _subscriptionTask;
                        Console.WriteLine("task stats " + i);

                        Console.WriteLine($"[{_localId}] :: finished await _subscriptionTask.Id={_subscriptionTask.Id}");
                        Console.Out.Flush();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{_localId}] :: exception _subscriptionTask.Id={_subscriptionTask.Id}" +
                                          Environment.NewLine + ex.Message);
                        Console.Out.Flush();
                        // just need to wait for it to end
                    }
                    finally
                    {
                        Console.WriteLine($"[{_localId}] :: done _subscriptionTask.Id={_subscriptionTask.Id} = status - " + _subscriptionTask.Status);
                        Console.Out.Flush();

                    }
                }
                Console.WriteLine($"[{_localId}] :: disposed ack is {_lastReceivedEtag}");
                Console.Out.Flush();

                CloseTcpClient(); // we disconnect immediately, freeing the subscription task

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

        public Task StartAsync()
        {
            if (_started)
                return Task.CompletedTask;

            if (_subscribers.Count == 0)
                throw new InvalidOperationException(
                    "No observers has been registered, did you forget to call Subscribe?");
            _started = true;
            var tcs = new TaskCompletionSource<object>();
            _subscriptionTask = Task.Run(async () =>
            {
                try
                {
                    return  await RunSubscriptionAsync(tcs);
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
            var connectionInfo = await _commands.GetTcpInfoAsync();
            var uri = new Uri(connectionInfo.Url);
            await _tcpClient.ConnectAsync(uri.Host, uri.Port);

            _tcpClient.NoDelay = true;
            _tcpClient.SendBufferSize = 32 * 1024;
            _tcpClient.ReceiveBufferSize = 4096;
            _networkStream = _tcpClient.GetStream();

            var ms = new MemoryStream();

            RavenJObject.FromObject(new TcpConnectionHeaderMessage
            {
                Operation = TcpConnectionHeaderMessage.OperationTypes.Subscription,
                DatabaseName = MultiDatabase.GetDatabaseName(_commands.Url)
            }).WriteTo(ms);

            RavenJObject.FromObject(_options).WriteTo(ms);
            ArraySegment<byte> bytes;
            ms.TryGetBuffer(out bytes);

            await _networkStream.WriteAsync(bytes.Array, bytes.Offset, bytes.Count);

            await _networkStream.FlushAsync();
            return _networkStream;
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
                            "Subscription #{0}. Subscriber threw an exception while proccessing OnError " + e, _options.SubscriptionId), ex);
                }
            }
        }

        private void AssertConnectionState(SubscriptionConnectionServerMessage connectionStatus)
        {
            if (connectionStatus.Type != SubscriptionConnectionServerMessage.MessageType.CoonectionStatus)
                throw new Exception("Server returned illegal type message when excpecting connection status, was: " + connectionStatus.Type);

            switch (connectionStatus.Status)
            {
                case SubscriptionConnectionServerMessage.ConnectionStatus.Accepted:
                    break;
                case SubscriptionConnectionServerMessage.ConnectionStatus.InUse:
                    throw new SubscriptionInUseException(
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it's in use and the connection strategy is {this._options.Strategy}");
                case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                    throw new SubscriptionClosedException(
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it was closed");
                case SubscriptionConnectionServerMessage.ConnectionStatus.NotFound:
                    throw new SubscriptionDoesNotExistException(
                        $"Subscription With Id {this._options.SubscriptionId} cannot be opened, because it does not exist");
                default:
                    throw new ArgumentException(
                        $"Subscription {this._options.SubscriptionId} could not be opened, reason: {connectionStatus.Status}");
            }
        }

        private async Task<int> ProccessSubscription(TaskCompletionSource<object> successfullyConnected)
        {
            try
            {
                _proccessingCts.Token.ThrowIfCancellationRequested();

                using (var tcpStream = await ConnectToServer().ConfigureAwait(false))
                using (var reader = new StreamReader(tcpStream))
                using (var jsonReader = new JsonTextReaderAsync(reader))
                {
                    _proccessingCts.Token.ThrowIfCancellationRequested();
                    var readObjectTask = ReadNextObject(jsonReader, false);
                    var done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                    if (done == _disposedTask.Task)
                        return -1;
                    var connectionStatus = await readObjectTask.ConfigureAwait(false);
                    if (_proccessingCts.IsCancellationRequested)
                        return -2;

                    AssertConnectionState(connectionStatus);

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(() => successfullyConnected.TrySetResult(null));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                    readObjectTask = ReadNextObject(jsonReader, false);


                    if (_proccessingCts.IsCancellationRequested)
                        return -3;

                    var incomingBatch = new List<RavenJObject>();
                    _lastReceivedEtag = 0;
                    bool waitingForAck = false;
                    while (_proccessingCts.IsCancellationRequested == false || waitingForAck)
                    {
                        Console.WriteLine($"[{_localId}] :: Status1 : {_proccessingCts.IsCancellationRequested} , {waitingForAck}");
                        Console.Out.Flush();

                        BeforeBatch();
                        bool endOfBatch = false;
                        while ((endOfBatch == false && _proccessingCts.IsCancellationRequested == false) || waitingForAck)
                        {
                            if (readObjectTask == null)
                                readObjectTask = ReadNextObject(jsonReader, waitingForAck);

                            Console.WriteLine($"[{_localId}] :: Status2 : {_proccessingCts.IsCancellationRequested} , {waitingForAck}");
                            Console.Out.Flush();

                            bool track = false;

                            done = await Task.WhenAny(readObjectTask, _disposedTask.Task).ConfigureAwait(false);
                            if (done == _disposedTask.Task)
                            {
                                Console.WriteLine($"[{_localId}] :: Status3 : {_proccessingCts.IsCancellationRequested} , {waitingForAck}");
                                Console.Out.Flush();

                                if (waitingForAck == false)
                                    break;

                                // await readObjectTask;

                                track = true;
                            }

                            Console.WriteLine($"done == _disposedTask.Task = {done == _disposedTask.Task}");

                            SubscriptionConnectionServerMessage receivedMessage = null;
                            receivedMessage = await readObjectTask.ConfigureAwait(false);

                            if (track)
                            {
                                Console.WriteLine("Client last message " + receivedMessage?.Type);
                                Console.WriteLine("Client last " + receivedMessage?.Data);
                                Console.Out.Flush();    
                            }


                            Console.WriteLine($"[{_localId}] :: Status4 : {_proccessingCts.IsCancellationRequested} , {waitingForAck} , {receivedMessage?.Type}");
                            Console.Out.Flush();

                            if (done == _disposedTask.Task)
                                waitingForAck = false; // we will only wait once

                            switch (receivedMessage?.Type)
                            {
                                case SubscriptionConnectionServerMessage.MessageType.Data:
                                    incomingBatch.Add(receivedMessage.Data);
                                    break;
                                case SubscriptionConnectionServerMessage.MessageType.EndOfBatch:
                                    endOfBatch = true;
                                    break;
                                case SubscriptionConnectionServerMessage.MessageType.Confirm:
                                    Console.WriteLine($"[{_localId}] :: CLIENT confirm " + _lastReceivedEtag);
                                    Console.Out.Flush();
                                    AfterAcknowledgment();
                                    AfterBatch(incomingBatch.Count);
                                    incomingBatch.Clear();
                                    waitingForAck = false;
                                    break;
                                case SubscriptionConnectionServerMessage.MessageType.Error:
                                    switch (receivedMessage.Status)
                                    {
                                        case SubscriptionConnectionServerMessage.ConnectionStatus.Closed:
                                            throw new SubscriptionClosedException(receivedMessage.Exception ?? string.Empty);
                                        default:
                                            throw new Exception($"Connection terminated by server. Exception: {receivedMessage.Exception ?? "None"}");
                                    }
                                                                        
                                default:
                                    throw new ArgumentException(
                                        $"Unrecognized message '{receivedMessage?.Type}' type received from server");
                            }
                            readObjectTask = null;
                        }

                        Console.WriteLine($"[{_localId}] :: checking for Cancel");
                        Console.Out.Flush();
                        if (_proccessingCts.IsCancellationRequested)
                            break;

                        foreach (var curDoc in incomingBatch)
                        {
                            NotifySubscribers(curDoc, out _lastReceivedEtag);
                        }

                        Console.WriteLine($"[{_localId}] :: client sending ACK : " + _lastReceivedEtag);
                        Console.Out.Flush();
                        SendAck(_lastReceivedEtag, tcpStream);
                        waitingForAck = true;
                        readObjectTask = ReadNextObject(jsonReader, true);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[{_localId}] :: OpearationCanceledException");
                Console.Out.Flush();

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                if (_proccessingCts.Token.IsCancellationRequested==false)
                    InformSubscribersOnError(ex);
                throw;
            }
            return -5;
        }

        private async Task<SubscriptionConnectionServerMessage> ReadNextObject(JsonTextReaderAsync jsonReader, bool waitForAck)
        {
            do
            {
                if ((_proccessingCts.IsCancellationRequested || _tcpClient.Connected == false) && waitForAck == false)
                    return null;
                jsonReader.ResetState();
            } while (await jsonReader.ReadAsync().ConfigureAwait(false) == false && 
            _proccessingCts.Token.IsCancellationRequested);// need to do that to handle the heartbeat whitespace 
            if (_proccessingCts.Token.IsCancellationRequested && waitForAck == false)
                return null;
            return (await RavenJObject.LoadAsync(jsonReader).ConfigureAwait(false)).JsonDeserialization<SubscriptionConnectionServerMessage>();
        }
        

        private void NotifySubscribers(RavenJObject curDoc, out long lastReceivedEtag)
        {
            T instance;
            var metadata = curDoc[Constants.Metadata.Key] as RavenJObject;
            lastReceivedEtag = metadata[Constants.Metadata.Etag].Value<long>();

            Console.WriteLine("------> DEUBG : " + lastReceivedEtag + " for " + metadata[Constants.Metadata.Id]);
            Console.Out.Flush();

            if (_isStronglyTyped)
            {
                instance = curDoc.Deserialize<T>(_conventions);

                var docId = metadata[Constants.Metadata.Id].Value<string>();

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
            Console.WriteLine("CLIENT SendAck " + lastReceivedEtag);
            Console.Out.Flush();
            RavenJObject.FromObject(new SubscriptionConnectionClientMessage
            {
                Etag = lastReceivedEtag,
                Type = SubscriptionConnectionClientMessage.MessageType.Acknowledge
            }).WriteTo(networkStream);
            networkStream.Flush();
        }

        private async Task<int> RunSubscriptionAsync(TaskCompletionSource<object> firstConnectionCompleted)
        {
            while (_proccessingCts.Token.IsCancellationRequested == false)
            {
                try
                {
                    CloseTcpClient();
                    Logger.Debug(string.Format("Subscription #{0}. Connecting to server...", _options.SubscriptionId));

                    _tcpClient = new TcpClient();
                    var rc = await ProccessSubscription(firstConnectionCompleted);
                    Console.WriteLine($"{_localId} :: ProccessSubscription returned " + rc);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{_localId} :: ProccessSubscription throw " + ex);

                    if (_proccessingCts.Token.IsCancellationRequested)
                    {
                        return -90;
                    }
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(() => firstConnectionCompleted.TrySetException(ex));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                    Logger.WarnException(
                        string.Format("Subscription #{0}. Pulling task threw the following exception", _options.SubscriptionId), ex);


                    if (await TryHandleRejectedConnection(ex, false).ConfigureAwait(false))
                    {
                        if (Logger.IsDebugEnabled)
                            Logger.Debug(string.Format("Subscription #{0}.", _options.SubscriptionId));
                        return -91;
                    }
                    await Task.Delay(_options.TimeToWaitBeforeConnectionRetryMilliseconds);
                }
            }
            if (_proccessingCts.Token.IsCancellationRequested)
                return -92;

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
            return -93;
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
            if (_networkStream != null)
            {
                try
                {
                    _networkStream.Dispose();
                    _networkStream = null;
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
    }
}
