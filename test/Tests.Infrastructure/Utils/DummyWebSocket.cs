using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Tests.Infrastructure.Utils
{
    internal class DummyWebSocket : WebSocket
    {
        private static WebSocketReceiveResult Result { get; } = new(1, WebSocketMessageType.Text, true);
        private readonly TaskCompletionSource<WebSocketReceiveResult> _completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly MemoryStream _stream = new();
        private int _isClosed;

        public override WebSocketCloseStatus? CloseStatus { get; }
        public override string CloseStatusDescription { get; }
        public override WebSocketState State { get; }
        public override string SubProtocol { get; }

        public override async Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (_isClosed == 1)
                throw new Exception("Closed");

            await _stream.WriteAsync(buffer.Array, 0, buffer.Count, cancellationToken);
        }

        public async Task<string> CloseAndGetLogsAsync()
        {
            Close();
            _stream.Seek(0, SeekOrigin.Begin);
            using StreamReader reader = new(_stream);
            return await reader.ReadToEndAsync();
        }

        public override void Dispose()
        {
            Close();
            _stream.Dispose();
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) => _completionSource.Task;

        private void Close()
        {
            if (Interlocked.CompareExchange(ref _isClosed, 1, 0) == 0)
                _completionSource.SetResult(Result);
        }

        public override void Abort() { }
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
