using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Sparrow.Logging;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;


namespace Raven.Server.TrafficWatch
{
    internal class TrafficWatchConnection : IDisposable
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<TrafficWatchConnection>("Server");

        readonly JsonContextPool _jsonContextPool = new JsonContextPool();

        private readonly WebSocket _websocket;
        public string TenantSpecific { get; set; }
        public bool IsAlive => _cancellationTokenSource.IsCancellationRequested == false;

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
                        await SendMessage(WebSocketHelper.Heartbeat).ConfigureAwait(false);
                        continue;
                    }

                    _manualResetEvent.Reset();

                    while (_msgs.TryDequeue(out TrafficWatchChange message))
                    {
                        if (_cancellationTokenSource.IsCancellationRequested)
                            break;

                        await SendMessage(ToByteArraySegment(message)).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when handling web socket connection", e);
                _cancellationTokenSource.Cancel();
            }
            finally
            {
                TrafficWatchManager.Disconnect(this);
                try
                {
                    await _websocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORMAL_CLOSE", _cancellationTokenSource?.Token ?? CancellationToken.None);
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
                [nameof(change.TimeStamp)] = change.TimeStamp,
                [nameof(change.RequestId)] = change.RequestId,
                [nameof(change.HttpMethod)] = change.HttpMethod,
                [nameof(change.ElapsedMilliseconds)] = change.ElapsedMilliseconds,
                [nameof(change.ResponseStatusCode)] = change.ResponseStatusCode,
                [nameof(change.RequestUri)] = change.RequestUri,
                [nameof(change.AbsoluteUri)] = change.AbsoluteUri,
                [nameof(change.DatabaseName)] = change.DatabaseName,
                [nameof(change.CustomInfo)] = change.CustomInfo,
                [nameof(change.Type)] = change.Type,
                [nameof(change.InnerRequestsCount)] = change.InnerRequestsCount
                //[nameof(change.QueryTimings)] = notification.QueryTimings
            };

            _bufferStream.SetLength(0);
            using (_jsonContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _bufferStream))
            {
                context.Write(writer, json);
                writer.Flush();

                _bufferStream.TryGetBuffer(out ArraySegment<byte> bytes);
                return bytes;
            }
        }

        private async Task SendMessage(ArraySegment<byte> message)
        {
            await _websocket.SendAsync(message, WebSocketMessageType.Text, true, _cancellationTokenSource.Token);
        }

        public void EnqueueMsg(TrafficWatchChange msg)
        {
            _msgs.Enqueue(msg);
            _manualResetEvent.Set();
        }

        public void Dispose()
        {
            _jsonContextPool.Dispose();
            _websocket.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }
}
