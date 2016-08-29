using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;
using Sparrow;
using Sparrow.Collections;
using Raven.Abstractions.Extensions;


namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchManager // TODO ADIADI :: different file and better class name
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchManager));

        // TODO ADIADI : do also CuncurrentDisctionary<tenant name, connection> and allow to traffic watch by tennat
        private static ConcurrentSet<TrafficWatchConnection> _serverHttpTrace = new ConcurrentSet<TrafficWatchConnection>();
        public static void AddConnection(TrafficWatchConnection connection)
        {
            _serverHttpTrace.Add(connection);
            Logger.Info($"TrafficWatch connection with Id={connection.Id} was opened");
        }

        public static void Disconnect(TrafficWatchConnection connection)
        {
            if (_serverHttpTrace.TryRemove(connection) != true)
            {
                Logger.Error($"Couldn't remove connection of TrafficWatch with Id={connection.Id}");
                return;
            }
            Logger.Info($"TrafficWatch connection with Id={connection.Id} was closed");
        }

        public static void DispatchMessage(TrafficWatchNotification trafficWatchData) // TODO ADIADI : distinquish between serverHttpTrace and tennat specific
        {
            foreach (var connection in _serverHttpTrace)
            {
                connection.EnqueMsg(trafficWatchData);
            }
        }
    }


    public class TrafficWatchConnection
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchConnection));

        private WebSocket _websocket;
        public string Id { get; }
        private readonly AsyncManualResetEvent _manualResetEvent;
        private CancellationTokenSource _cancellationTokenSource;

        private readonly byte[] _heartbeatMessage = Encoding.UTF8.GetBytes("{'Type': 'Heartbeat','Time': '");
        private byte[] _heartbeatMessageBuffer;

        public long CoolDownWithDataLossInMiliseconds { get; set; } // TODO ADIADI :: from where we call this ?
        private long _lastMessageSentTick = 0;
        private TrafficWatchNotification _lastMessageEnqueuedAndNotSent = null;
        private readonly ConcurrentQueue<TrafficWatchNotification> _msgs = new ConcurrentQueue<TrafficWatchNotification>();


        public TrafficWatchConnection(WebSocket webSocket, string id, CancellationToken ctk)
        {
            _websocket = webSocket;
            _manualResetEvent = new AsyncManualResetEvent(ctk);//, disconnectBecauseOfGcToken); // TODO ADIADI : gc close or others.. 
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ctk);
            _heartbeatMessageBuffer = new byte[_heartbeatMessage.Length + 32]; // add some sapce to dynamically add Time value
            Buffer.BlockCopy(_heartbeatMessageBuffer, 0, _heartbeatMessage, 0, _heartbeatMessage.Length);
            Id = id;
        }


        public async Task StartSendingNotifications()
        {
            try
            {
                CreateWaitForClientCloseTask();

                while (_cancellationTokenSource.IsCancellationRequested == false)
                {
                    var result = await _manualResetEvent.WaitAsync(TimeSpan.FromMilliseconds(5000)).ConfigureAwait(false);
                    if (_cancellationTokenSource.IsCancellationRequested)
                        break;

                    if (result == false)
                    {
                        // TODO ADIADI : do this on unmanaged or byte by byte: and in different func 
                        var utcNow = Encoding.UTF8.GetBytes(SystemTime.UtcNow.ToString(CultureInfo.InvariantCulture) + "'}");
                        Debug.Assert(utcNow.Length < 32); // utcNow should not exceed 32 (or else _heartbeatMessageBuffer should be increased)
                        Buffer.BlockCopy(_heartbeatMessageBuffer, _heartbeatMessage.Length, utcNow, 0, utcNow.Length);
                        Array.Clear(_heartbeatMessageBuffer, _heartbeatMessage.Length + utcNow.Length, _heartbeatMessageBuffer.Length - (_heartbeatMessage.Length + utcNow.Length));

                        await SendMessage(_heartbeatMessageBuffer).ConfigureAwait(false);

                        if (_lastMessageEnqueuedAndNotSent != null)
                        {
                            await SendMessage(toByteArraySegment(_lastMessageEnqueuedAndNotSent)).ConfigureAwait(false);
                            _lastMessageEnqueuedAndNotSent = null;
                            _lastMessageSentTick = Environment.TickCount;
                        }

                        continue;
                    }

                    _manualResetEvent.Reset();

                    TrafficWatchNotification message;
                    while (_msgs.TryDequeue(out message))
                    {
                        if (_cancellationTokenSource.IsCancellationRequested)
                            break;

                        if (CoolDownWithDataLossInMiliseconds > 0 && Environment.TickCount - _lastMessageSentTick < CoolDownWithDataLossInMiliseconds)
                        {
                            _lastMessageEnqueuedAndNotSent = message;
                            continue;
                        }

                        await SendMessage(toByteArraySegment(message)).ConfigureAwait(false);
                        _lastMessageEnqueuedAndNotSent = null;
                        _lastMessageSentTick = Environment.TickCount;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Info("Error when handling web socket connection", e);
                _cancellationTokenSource?.Cancel();
            }
            finally
            {
                TrafficWatchManager.Disconnect(this); // TODO ADIADI : from a first look - it doen't required here.  check..
            }
        }

        private void CreateWaitForClientCloseTask()
        {
            new Task(async () =>
            {
                var buffer = new ArraySegment<byte>(new byte[1024]);

                while (true)
                {
                    try
                    {
                        bool cancelled = false;
                        WebSocketReceiveResult receiveResult = null;
                        try
                        {
                            receiveResult = await _websocket.ReceiveAsync(buffer, _cancellationTokenSource.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            cancelled = true;
                        }
                        if (cancelled)
                        {
                            try
                            {
                                await _websocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closed for GC release", CancellationToken.None).ConfigureAwait(false);
                                _manualResetEvent.Set();
                            }
                            catch (Exception e)
                            {
                                Logger.WarnException("Error when closing connection web socket transport", e);
                            }
                            return;
                        }

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            if (receiveResult.CloseStatus == WebSocketCloseStatus.NormalClosure && receiveResult.CloseStatusDescription == "CLOSE_NORMAL") // TODO ADIADI :: is it?
                            {
                                await _websocket.CloseAsync(receiveResult.CloseStatus.Value, receiveResult.CloseStatusDescription, _cancellationTokenSource.Token).ConfigureAwait(false);
                                _manualResetEvent.Set();
                            }

                            //At this point the WebSocket is in a 'CloseReceived' state, so there is no need to continue waiting for messages
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        Logger.WarnException("Error when receiving message from web socket transport", e);
                        return;
                    }
                }
            }).Start();
        }

        private ArraySegment<byte> toByteArraySegment(TrafficWatchNotification notification)
        {
            var ravenJObject = new RavenJObject
            {
                ["TimeStamp"] = notification.TimeStamp,
                ["RequestId"] = notification.RequestId,
                ["HttpMethod"] = notification.HttpMethod,
                ["ElapsedMilliseconds"] = notification.ElapsedMilliseconds,
                ["ResponseStatusCode"] = notification.ResponseStatusCode,
                ["RequestUri"] = notification.RequestUri,
                ["AbsoluteUri"] = notification.AbsoluteUri,
                ["TenantName"] = notification.TenantName,
                ["CustomInfo"] = notification.CustomInfo,
                ["InnerRequestsCount"] = notification.InnerRequestsCount,
                ["QueryTimings"] = notification.QueryTimings
            };

            var stream = new MemoryStream();
            ravenJObject.WriteTo(stream);
            ArraySegment<byte> bytes;
            stream.TryGetBuffer(out bytes);
            return bytes;
        }

        private async Task SendMessage(byte[] message)
        {
            ArraySegment<byte> arraySegment = new ArraySegment<byte>(message);
            await SendMessage(arraySegment);
        }

        private async Task SendMessage(ArraySegment<byte> message)
        {
            await _websocket.SendAsync(message, WebSocketMessageType.Text, true, _cancellationTokenSource.Token);
        }

        public void EnqueMsg(TrafficWatchNotification msg)
        {
            _msgs.Enqueue(msg);
            _manualResetEvent.Set();
        }
    }
}
