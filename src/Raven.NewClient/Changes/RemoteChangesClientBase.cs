using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Platform;
using Raven.NewClient.Client.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Changes
{
    public abstract class RemoteChangesClientBase<TChangesApi, TConnectionState, TConventions> : IDisposable, IConnectableChanges<TChangesApi>
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
        private int _counter;
        private ConcurrentDictionary<int, TaskCompletionSource<object>> _sendConfirmations = new ConcurrentDictionary<int, TaskCompletionSource<object>>(); 

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
                throw new InvalidCastException(string.Format("The derived class does not implements {0}. Make sure the {0} interface is implemented by this class.", typeof(TChangesApi).Name));

            ConnectionStatusChanged = LogOnConnectionStatusChanged;

            id = Interlocked.Increment(ref connectionCounter) + "/" + Base62Util.Base62Random();

            this.url = url;
            this.credentials = new OperationCredentials(apiKey, credentials);
            this.onDispose = onDispose;
            Conventions = conventions;
            webSocket = new RavenClientWebSocket();

            ConnectionTask = EstablishConnection()
                .ObserveException()
                .ContinueWith(task =>
                {
                    task.AssertNotFailed();

                    Task.Run(Receive);

                    return this as TChangesApi;
                });
        }

        private async Task EstablishConnection()
        {
            if (disposed)
                return;

            var uri = new Uri(url.ToWebSocketPath() + "/changes");
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
            foreach (var value in Counters.ValuesSnapshot)
            {
                value.Task.Wait();
            }
        }

        private async Task Receive()
        {
            try
            {
                using (var ms = new MemoryStream())
                {
                    ms.SetLength(4096);
                    while (webSocket.State == WebSocketState.Open)
                    {
                        if (ms.Length > 4096*16)
                            ms.SetLength(4096);

                        ms.Position = 0;
                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(bytes.Array, (int)ms.Position, (int)(ms.Length - ms.Position)),
                            disposedToken.Token);
                        ms.Position = result.Count;
                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None);
                            return;
                        }

                        while (result.EndOfMessage == false)
                        {
                            if (ms.Length - ms.Position < 1024)
                                ms.SetLength(ms.Length + 4096);
                            ms.TryGetBuffer(out bytes);
                            result = await webSocket.ReceiveAsync(new ArraySegment<byte>(bytes.Array, (int)ms.Position, (int)(ms.Length - ms.Position)), CancellationToken.None);
                            ms.Position += result.Count;
                        }

                        ms.SetLength(ms.Position);
                        ms.Position = 0;

                        using (var reader = new StreamReader(ms, Encoding.UTF8, true, 1024, true))
                        using (var jsonReader = new RavenJsonTextReader(reader)
                        {
                            SupportMultipleContent = true
                        })
                        while (jsonReader.Read())
                        {
                            var ravenJObject = RavenJObject.Load(jsonReader);
                            HandleRevicedNotification(ravenJObject);
                        }
                    }
                }
            }
            catch (WebSocketException ex)
            {
                logger.DebugException("Failed to receive a message, client was probably disconnected", ex);
            }
        }

        private void HandleRevicedNotification(RavenJObject ravenJObject)
        {
            var value = ravenJObject.Value<RavenJObject>("Value");
            var type = ravenJObject.Value<string>("Type");
            if (logger.IsDebugEnabled)
                logger.Debug("Got notification from {0} id {1} of type {2}", url, id, ravenJObject.ToString());

            switch (type)
            {
                case "Disconnect":
                    webSocket.Dispose();
                    break;
                case "Confirm":
                    TaskCompletionSource<object> source;
                    if (_sendConfirmations.TryRemove(ravenJObject.Value<int>("CommandId"), out source))
                        source.TrySetResult(null);
                    break;
                case "Initialized":
                case "Heartbeat":
                    throw new NotSupportedException(); // Should be deleted
                
                default:
                    NotifySubscribers(type, value, Counters.ValuesSnapshot);
                    break;
            }
        }

        protected async Task Send(string command, string commandParameter)
        {
            logger.Info("Sending command {0} - {1} to {2} with id {3}", command, commandParameter, url, id);

            var commandId = Interlocked.Increment(ref _counter);
            var ravenJObject = new RavenJObject
            {
                ["Command"] = command,
                ["Param"] = commandParameter,
                ["CommandId"] = commandId
            };
            var tcs = new TaskCompletionSource<object>();
            _sendConfirmations[commandId] = tcs;
            var stream = new MemoryStream();
            ravenJObject.WriteTo(stream);
            ArraySegment<byte> bytes;
            stream.TryGetBuffer(out bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None).ConfigureAwait(false);

            await tcs.Task.ConfigureAwait(false);
        }

        private readonly CancellationTokenSource disposedToken = new CancellationTokenSource();

        public void Dispose()
        {
            if (disposed)
                return;

            DisposeAsync().Wait();
        }

        private volatile bool disposed;
        private readonly RavenClientWebSocket webSocket;

        public Task DisposeAsync()
        {
            if (disposed)
                return new CompletedTask();
            disposed = true;

            disposedToken.Cancel();
            onDispose();

            return webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed by the client", CancellationToken.None)
                .ContinueWith(_ =>
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

            // TODO: RenewConnection();
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
        protected abstract void NotifySubscribers(string type, RavenJObject value, List<TConnectionState> connections);

        public virtual void OnCompleted()
        { }

        protected TConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, Action afterConnection, Action beforeDisconnect, string value)
        {
            var counter = Counters.GetOrAdd(name, s =>
            {
                Func<Task> onZero = () =>
                {
                    beforeDisconnect();
                    
                    Counters.Remove(name);
                    return Send(unwatchCommand, value);
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
