using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;

namespace SlowTests.SparrowTests;

public class LogTestsHelper
{
    public class DummyWebSocket : WebSocket
    {
        private bool _close;
        TaskCompletionSource<WebSocketReceiveResult> _tcs = new TaskCompletionSource<WebSocketReceiveResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        public string LogsReceived { get; private set; } = "";

        public void Close() => _close = true;

        public Func<Task<WebSocketReceiveResult>> ReceiveAsyncFunc { get; set; }

        public override void Abort()
        {
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public override void Dispose()
        {
            _tcs.TrySetResult(new WebSocketReceiveResult(1, WebSocketMessageType.Text, true, WebSocketCloseStatus.NormalClosure, string.Empty));
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) => ReceiveAsyncFunc != null ? ReceiveAsyncFunc() : _tcs.Task;

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (_close)
                throw new Exception("Closed");
            LogsReceived += Encodings.Utf8.GetString(buffer.ToArray());
            return Task.CompletedTask;
        }

        public override WebSocketCloseStatus? CloseStatus { get; }
        public override string CloseStatusDescription { get; }
        public override WebSocketState State { get; }
        public override string SubProtocol { get; }
    }

}
