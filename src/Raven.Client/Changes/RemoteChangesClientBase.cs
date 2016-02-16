using System;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Changes
{
    public abstract class RemoteChangesClientBase<TChangesApi, TConnectionState, TConventions> : IDisposable, IObserver<string>, IConnectableChanges<TChangesApi>
                                where TConnectionState : class, IChangesConnectionState
                                where TChangesApi : class, IConnectableChanges
                                where TConventions : ConventionBase
    {
        private static readonly ILog logger = LogManager.GetLogger(typeof(RemoteChangesClientBase<TChangesApi, TConnectionState, TConventions>));

        private readonly string url;
        private readonly OperationCredentials credentials;
        private readonly Action onDispose;

        private static int connectionCounter;
        private readonly string id;

        // This is the StateCounters, it is not related to the counters database
        protected readonly AtomicDictionary<TConnectionState> Counters = new AtomicDictionary<TConnectionState>(StringComparer.OrdinalIgnoreCase);        

        protected RemoteChangesClientBase(
            string url,
            string apiKey,
            ICredentials credentials,
            TConventions conventions,
            Action onDispose)
        {
            // Precondition
            var api = this as TChangesApi;
            if (api == null)
                throw new InvalidCastException(string.Format("The derived class does not implements {0}. Make sure the {0} interface is implemented by this class.", typeof (TChangesApi).Name));

            ConnectionStatusChanged = LogOnConnectionStatusChanged;

            id = Interlocked.Increment(ref connectionCounter) + "/" + Base62Util.Base62Random();

            this.url = url;
            this.credentials = new OperationCredentials(apiKey, credentials);
            this.onDispose = onDispose;
            Conventions = conventions;
            webSocket = new ClientWebSocket();

            ConnectionTask = EstablishConnection()
                .ObserveException()
                .ContinueWith(task =>
                {
                    task.AssertNotFailed();
                    return this as TChangesApi;
                });
        }

        private async Task EstablishConnection()
        {
            if (disposed)
                return;

            var uri = new Uri(url + "/changes");
            logger.Info("Trying to connect to {0}", uri);
            await webSocket.ConnectAsync(uri, CancellationToken.None);
        }

        protected TConventions Conventions { get; private set; }

        public bool Connected { get; private set; }
        public event EventHandler ConnectionStatusChanged;

        private void LogOnConnectionStatusChanged(object sender, EventArgs eventArgs)
        {
            logger.Info("Connection ({1}) status changed, new status: {0}", Connected, url);
        }


        public Task<TChangesApi> ConnectionTask { get; private set; }

        public void WaitForAllPendingSubscriptions()
        {
            foreach (var kvp in Counters)
            {
                kvp.Value.Task.Wait();
            }
        }

        static UTF8Encoding encoder = new UTF8Encoding();

        private const int receiveChunkSize = 256;

        private static async Task Receive(ClientWebSocket webSocket)
        {
            byte[] buffer = new byte[receiveChunkSize];
            while (webSocket.State == WebSocketState.Open)
            {
                var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
                }
                else
                {
                    
                }
            }
        }

        private void ClientSideHeartbeat(object _)
        {
            TimeSpan elapsedTimeSinceHeartbeat = SystemTime.UtcNow - lastHeartbeat;
            if (elapsedTimeSinceHeartbeat.TotalSeconds < 45)
                return;
            OnError(new TimeoutException("Over 45 seconds have passed since we got a server heartbeat, even though we should get one every 10 seconds or so.\r\n" +
                                         "This connection is now presumed dead, and will attempt reconnection"));
        }

        protected Task Send(string command, string value)
        {
            lock (this)
            {
                logger.Info("Sending command {0} - {1} to {2} with id {3}", command, value, url, id);
                var sendTask = lastSendTask;
                if (sendTask != null)
                {
                    return sendTask.ContinueWith(_ =>
                    {
                        Send(command, value);
                    });
                }

                try
                {
                    var sendUrl = url + "/changes/config?id=" + id + "&command=" + command;
                    if (string.IsNullOrEmpty(value) == false)
                        sendUrl += "&value=" + Uri.EscapeUriString(value);

                    var requestParams = new CreateHttpJsonRequestParams(null, sendUrl, HttpMethods.Get, credentials, Conventions)
                    {
                        AvoidCachingRequest = true
                    };
                    var request = jsonRequestFactory.CreateHttpJsonRequest(requestParams);
                    return lastSendTask =
                        request.ExecuteRequestAsync()
                            .ObserveException()
                            .ContinueWith(task =>
                            {
                                lastSendTask = null;
                                request.Dispose();
                            });
                }
                catch (Exception e)
                {
                    return new CompletedTask(e).Task.ObserveException();
                }
            }
        }

        public void Dispose()
        {
            if (disposed)
                return;

            DisposeAsync().Wait();
        }

        private volatile bool disposed;
        private readonly ClientWebSocket webSocket;

        public Task DisposeAsync()
        {
            if (disposed)
                return new CompletedTask();
            disposed = true;
            onDispose();


            return Send("disconnect", null).
                ContinueWith(_ =>
                {
                    try
                    {
                        webSocket?.Dispose();
                    }
                    catch (Exception e)
                    {
                        logger.ErrorException("Got error from server connection for " + url + " on id " + id, e);

                    }
                });
        }


        public virtual void OnError(Exception error)
        {
            logger.ErrorException("Got error from server connection for " + url + " on id " + id, error);

            RenewConnection();
        }

        public void OnNext(string dataFromConnection)
        {
            lastHeartbeat = SystemTime.UtcNow;

            var ravenJObject = RavenJObject.Parse(dataFromConnection);
            var value = ravenJObject.Value<RavenJObject>("Value");
            var type = ravenJObject.Value<string>("Type");
            if (logger.IsDebugEnabled)
            logger.Debug("Got notification from {0} id {1} of type {2}", url, id, dataFromConnection);

            switch (type)
            {
                case "Disconnect":
                    connection?.Dispose();
                    RenewConnection();
                    break;
                case "Initialized":
                case "Heartbeat":
                    break;
                default:
                    NotifySubscribers(type, value, Counters.Snapshot);
                    break;
            }
        }

        protected Task AfterConnection(Func<Task> action)
        {
            return ConnectionTask.ContinueWith(task =>
            {
                task.AssertNotFailed();
                return action();
            })
            .Unwrap();
        }

        protected abstract Task SubscribeOnServer();
        protected abstract void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, TConnectionState>> connections);

        public virtual void OnCompleted()
        { }

        protected TConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, Action afterConnection, Action beforeDisconnect, string value)
        {
            var counter = Counters.GetOrAdd(name, s =>
            {
                Action onZero = () =>
                {
                    beforeDisconnect();
                    Send(unwatchCommand, value);
                    Counters.Remove(name);
                };

                Func<TConnectionState, Task> ensureConnection = existingConnectionState =>
                {
                    TConnectionState _;
                    if (Counters.TryGetValue(name, out _))
                        return _.Task;

                    Counters.GetOrAdd(name, x => existingConnectionState);

                    return AfterConnection(() =>
                    {
                        afterConnection();
                        return Send(watchCommand, value);
                    });
                };

                var counterSubscriptionTask = AfterConnection(() =>
                {
                    afterConnection();
                    return Send(watchCommand, value);
                });

                return CreateTConnectionState(onZero, ensureConnection, counterSubscriptionTask);
            });

            return counter;
    }

        private static TConnectionState CreateTConnectionState(params object[] args)
        {
            return (TConnectionState)Activator.CreateInstance(typeof(TConnectionState), args);
}
    }
}
