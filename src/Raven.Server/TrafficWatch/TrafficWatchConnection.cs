using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;


namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchConnection
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchConnection));
        readonly JsonContextPool _jsonContextPool = new JsonContextPool();

        private readonly WebSocket _websocket;
        public string Id { get; }
        public string TenantSpecific { get; set; }

        private readonly AsyncManualResetEvent _manualResetEvent;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private readonly byte[] _heartbeatMessage = Encoding.UTF8.GetBytes("{'Type': 'Heartbeat','Time': '");
        private readonly byte[] _heartbeatMessageBuffer;

        private readonly ConcurrentQueue<TrafficWatchNotification> _msgs = new ConcurrentQueue<TrafficWatchNotification>();
        private int _timeout;

        public TrafficWatchConnection(WebSocket webSocket, string id, CancellationToken ctk, string resourceName, int timeout)
        {
            _websocket = webSocket;
            _manualResetEvent = new AsyncManualResetEvent(ctk);
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ctk);
            _heartbeatMessageBuffer = new byte[_heartbeatMessage.Length + 32]; // add some sapce to dynamically add Time value
            Buffer.BlockCopy(_heartbeatMessage, 0, _heartbeatMessageBuffer, 0,  _heartbeatMessage.Length);
            Id = id;
            TenantSpecific = resourceName;
            _timeout = timeout;
        }

        public async Task StartSendingNotifications()
        {
            try
            {
                var sp = Stopwatch.StartNew();
                while (_cancellationTokenSource.IsCancellationRequested == false)
                {
                    var result = await _manualResetEvent.WaitAsync(TimeSpan.FromMilliseconds(5000)).ConfigureAwait(false);
                    if (_cancellationTokenSource.IsCancellationRequested)
                        break;

                    if (result == false)
                    {
                        var utcNow = Encoding.UTF8.GetBytes(SystemTime.UtcNow + "'}");
                        Debug.Assert(utcNow.Length < 32); // utcNow should not exceed 32 (or else _heartbeatMessageBuffer should be increased)
                        Buffer.BlockCopy(utcNow, 0, _heartbeatMessageBuffer, _heartbeatMessage.Length, utcNow.Length);
                        Array.Clear(_heartbeatMessageBuffer, _heartbeatMessage.Length + utcNow.Length, _heartbeatMessageBuffer.Length - (_heartbeatMessage.Length + utcNow.Length));

                        await SendMessage(_heartbeatMessageBuffer).ConfigureAwait(false);
                    }

                    _manualResetEvent.Reset();

                    TrafficWatchNotification message;
                    while (_msgs.TryDequeue(out message))
                    {
                        if (_cancellationTokenSource.IsCancellationRequested)
                            break;

                        await SendMessage(ToByteArraySegment(message)).ConfigureAwait(false);
                    }

                    if (_timeout > 0 && sp.ElapsedMilliseconds/1000 > _timeout)
                        break;
                }
            }
            catch (Exception e)
            {
                Logger.Info("Error when handling web socket connection", e);
                _cancellationTokenSource?.Cancel();
            }
            finally
            {
                TrafficWatchManager.Disconnect(this);
                try
                {
                    await
                        _websocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORNAL_CLOSE", _cancellationTokenSource?.Token ?? CancellationToken.None);
                }
                catch
                {
                    // ignore
                }
            }
        }

        private ArraySegment<byte> ToByteArraySegment(TrafficWatchNotification notification)
        {
            var json = new DynamicJsonValue
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
                //["QueryTimings"] = notification.QueryTimings // TODO :: implement this
            };

            var stream = new MemoryStream();
            JsonOperationContext context;
            using (_jsonContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                context.Write(writer, json);
                writer.Flush();

                ArraySegment<byte> bytes;
                stream.TryGetBuffer(out bytes);
                return bytes;
            }
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
