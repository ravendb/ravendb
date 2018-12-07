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
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<TrafficWatchConnection>("Server");

        private readonly WebSocket _webSocket;
        private readonly JsonOperationContext _context;
        public string TenantSpecific { get; set; }
        public bool IsAlive => _receive.IsCompleted == false &&
                               _cancellationTokenSource.IsCancellationRequested == false;

        private readonly AsyncManualResetEvent _manualResetEvent;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task<WebSocketReceiveResult> _receive;

        private readonly ConcurrentQueue<TrafficWatchChange> _messages = new ConcurrentQueue<TrafficWatchChange>();
        private readonly MemoryStream _bufferStream = new MemoryStream();

        private bool _disposed;

        public TrafficWatchConnection(WebSocket webSocket, string resourceName, JsonOperationContext context, CancellationToken ctk)
        {
            _webSocket = webSocket;
            TenantSpecific = resourceName;
            _context = context;

            _manualResetEvent = new AsyncManualResetEvent(ctk);
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ctk);

            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            _receive = _webSocket.ReceiveAsync(receiveBuffer, _cancellationTokenSource.Token);
        }

        public async Task StartSendingNotifications()
        {
            try
            {
                while (_cancellationTokenSource.IsCancellationRequested == false)
                {
                    var result = await _manualResetEvent.WaitAsync(TimeSpan.FromMilliseconds(5000)).ConfigureAwait(false);
                    if (IsAlive == false)
                        return;

                    if (result == false)
                    {
                        await SendMessage(WebSocketHelper.Heartbeat).ConfigureAwait(false);
                        continue;
                    }

                    _manualResetEvent.Reset();

                    while (_messages.TryDequeue(out var message))
                    {
                        if (IsAlive == false)
                            return;

                        await SendMessage(ToByteArraySegment(message)).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error when handling web socket connection", e);
            }
            finally
            {
                TrafficWatchManager.Disconnect(this);

                try
                {
                    if (_disposed == false)
                        await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "NORMAL_CLOSE", _cancellationTokenSource.Token);
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
                [nameof(change.CustomInfo)] = change.CustomInfo
            };

            _bufferStream.SetLength(0);
            using (var writer = new BlittableJsonTextWriter(_context, _bufferStream))
            {
                _context.Write(writer, json);
                writer.Flush();

                _bufferStream.TryGetBuffer(out var bytes);
                return bytes;
            }
        }

        private async Task SendMessage(ArraySegment<byte> message)
        {
            await _webSocket.SendAsync(message, WebSocketMessageType.Text, true, _cancellationTokenSource.Token);
        }

        public void EnqueueMsg(TrafficWatchChange msg)
        {
            _messages.Enqueue(msg);
            _manualResetEvent.Set();
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            _webSocket.Dispose();
            _cancellationTokenSource.Dispose();
        }
    }
}
