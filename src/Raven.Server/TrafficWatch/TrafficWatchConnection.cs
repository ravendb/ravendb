﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;

namespace Raven.Server.TrafficWatch
{

    internal sealed class TrafficWatchConnection
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<TrafficWatchConnection>();

        private readonly WebSocket _webSocket;
        private readonly JsonOperationContext _context;
        public string TenantSpecific { get; set; }

        public bool IsAlive => _receive.IsCompleted == false &&
                               _cancellationTokenSource.IsCancellationRequested == false;

        private readonly AsyncManualResetEvent _manualResetEvent;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly Task<WebSocketReceiveResult> _receive;

        private readonly ConcurrentQueue<TrafficWatchChangeBase> _messages = new ConcurrentQueue<TrafficWatchChangeBase>();
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
                    _context.Reset();
                    _context.Renew();

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
                        _context.Reset();
                        _context.Renew();
                        if (IsAlive == false)
                            return;

                        await SendMessage(await ToByteArraySegmentAsync(message)).ConfigureAwait(false);
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

        private async Task<ArraySegment<byte>> ToByteArraySegmentAsync(TrafficWatchChangeBase change)
        {
            var json = change.ToJson();

            _bufferStream.SetLength(0);
            await using (var writer = new AsyncBlittableJsonTextWriter(_context, _bufferStream))
            {
                _context.Write(writer, json);
                await writer.FlushAsync();

                _bufferStream.TryGetBuffer(out var bytes);
                return bytes;
            }
        }

        private async Task SendMessage(ArraySegment<byte> message)
        {
            await _webSocket.SendAsync(message, WebSocketMessageType.Text, true, _cancellationTokenSource.Token);
        }

        public void EnqueueMsg(TrafficWatchChangeBase msg)
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
            _manualResetEvent.Dispose();
        }
    }
}
