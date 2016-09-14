using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Json;
using Raven.Client.Data;
using Raven.Client.Document;
using Sparrow;

namespace FastTests.Utils
{
    public class FakeWebSocket : WebSocket
    {
        public override void Abort()
        {
            throw new NotSupportedException("Feel free to implement me when you need this method in test");
        }

        public override void Dispose()
        {
            throw new NotSupportedException("Feel free to implement me when you need this method in test");
        }

        public override WebSocketCloseStatus? CloseStatus { get; }
        public override string CloseStatusDescription { get; }
        public override WebSocketState State { get; }
        public override string SubProtocol { get; }

        public event Action<WebSocketMessage> OnMessageSent = message => { };

        public BlockingCollection<ArraySegment<byte>> ReceiveQueue { get; } = new BlockingCollection<ArraySegment<byte>>();

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            var newBuffer = new byte[buffer.Count];
            Array.Copy(buffer.Array, buffer.Offset, newBuffer, 0, buffer.Count);

            OnMessageSent(new WebSocketMessage
            {
                Buffer = new ArraySegment<byte>(newBuffer),
                EndOfMessage = endOfMessage,
                MessageType = messageType
            });
            return Task.CompletedTask;
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Feel free to implement me when you need this method in test");
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Feel free to implement me when you need this method in test");
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Feel free to implement me when you need this method in test");
        }
    }

    public class WebSocketMessage
    {
        public ArraySegment<byte> Buffer;
        public WebSocketMessageType MessageType;
        public bool EndOfMessage;

        public T DeserializeMessage<T>()
        {
            using (var ms = new MemoryStream(Buffer.Array, Buffer.Offset, Buffer.Count))
            using (var reader = new StreamReader(ms, Encoding.UTF8, true, 1024, true))
            using (var jsonReader = new RavenJsonTextReader(reader)
            {
                SupportMultipleContent = true
            })
            {
                jsonReader.Read();
                return new DocumentConvention().CreateSerializer().Deserialize<T>(jsonReader);
            }
        }
    }
}