using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Logging;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;


namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchConnection:IDisposable
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(TrafficWatchConnection));
        readonly JsonContextPool _jsonContextPool = new JsonContextPool();

        private readonly WebSocket _websocket;
        public string TenantSpecific { get; set; }
        public bool IsAlive => _cancellationTokenSource.IsCancellationRequested == false;

        private static readonly byte[] HeartbeatMessage = {(byte) '\r', (byte) '\n'};
        private readonly AsyncManualResetEvent _manualResetEvent;
        private readonly CancellationTokenSource _cancellationTokenSource;


        private readonly ConcurrentQueue<TrafficWatchChange> _msgs = new ConcurrentQueue<TrafficWatchChange>();
        private readonly MemoryStream _bufferStream = new MemoryStream();

        public TrafficWatchConnection(WebSocket webSocket, CancellationToken ctk, string resourceName)
        {
            _websocket = webSocket;
            _manualResetEvent = new AsyncManualResetEvent(ctk);
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ctk);
            TenantSpecific = resourceName;
        }

        public async Task StartSendingNotifications()
        {
            try
            {
                while (_cancellationTokenSource.IsCancellationRequested == false)
                {
                    var result = await _manualResetEvent.WaitAsync(TimeSpan.FromMilliseconds(5000)).ConfigureAwait(false);
                    if (_cancellationTokenSource.IsCancellationRequested)
                        break;

                    if (result == false)
                    {
                        await SendMessage(HeartbeatMessage).ConfigureAwait(false);
                        continue;
                    }

                    _manualResetEvent.Reset();

                    TrafficWatchChange message;
                    while (_msgs.TryDequeue(out message))
                    {
                        if (_cancellationTokenSource.IsCancellationRequested)
                            break;

                        await SendMessage(ToByteArraySegment(message)).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Info("Error when handling web socket connection", e);
                _cancellationTokenSource.Cancel();
            }
            finally
            {
                TrafficWatchManager.Disconnect(this);
                try
                {
                    await _websocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORNAL_CLOSE", _cancellationTokenSource?.Token ?? CancellationToken.None);
                }
                catch
                {
                    // ignore
                }
            }
        }

        private ArraySegment<byte> ToByteArraySegment(TrafficWatchChange change)
        {
            var json = new DynamicJsonValue
            {
                ["TimeStamp"] = change.TimeStamp,
                ["RequestId"] = change.RequestId,
                ["HttpMethod"] = change.HttpMethod,
                ["ElapsedMilliseconds"] = change.ElapsedMilliseconds,
                ["ResponseStatusCode"] = change.ResponseStatusCode,
                ["RequestUri"] = change.RequestUri,
                ["AbsoluteUri"] = change.AbsoluteUri,
                ["TenantName"] = change.TenantName,
                ["CustomInfo"] = change.CustomInfo,
                ["InnerRequestsCount"] = change.InnerRequestsCount,
                //["QueryTimings"] = notification.QueryTimings // TODO :: implement this
            };

            _bufferStream.SetLength(0);
            JsonOperationContext context;
            using (_jsonContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, _bufferStream))
            {
                context.Write(writer, json);
                writer.Flush();

                ArraySegment<byte> bytes;
                _bufferStream.TryGetBuffer(out bytes);
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

        public void EnqueMsg(TrafficWatchChange msg)
        {
            _msgs.Enqueue(msg);
            _manualResetEvent.Set();
        }

        public void Dispose()
        {
            _jsonContextPool.Dispose();
            
            _websocket.Dispose();
        }
    }
}
