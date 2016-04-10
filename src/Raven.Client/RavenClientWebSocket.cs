using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document
{
    public class RavenClientWebSocket : WebSocket, IDisposable
    {
        const string Magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        ClientWebSocketOptions options;
        WebSocketState state;
        string subProtocol;

        Random random = new Random();

        const int HeaderMaxLength = 14;
        byte[] headerBuffer;
        byte[] sendBuffer;
        long remaining;
        private TcpClient _connection;
        private NetworkStream _networkStream;

        public RavenClientWebSocket()
        {
            _connection = new TcpClient();
            state = WebSocketState.None;
            headerBuffer = new byte[HeaderMaxLength];
        }

        public override void Dispose()
        {

        }

        public override WebSocketState State => state;

        public override WebSocketCloseStatus? CloseStatus
        {
            get
            {
                if (state != WebSocketState.Closed)
                    return null;
                return WebSocketCloseStatus.Empty;
            }
        }

        public override string CloseStatusDescription => null;

        public override string SubProtocol => subProtocol;

        public async Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            Console.WriteLine("ConnectAsync :: " + uri);
            state = WebSocketState.Connecting;
            if (uri.Scheme == "wss")
                throw new NotSupportedException("https is not supported");

            var port = uri.Port;
            if (port == 0)
                port = 80;

            await _connection.ConnectAsync(uri.Host, port);

            Console.WriteLine("after _connection.ConnectAsync");

            _networkStream = _connection.GetStream();
            var secKey = Convert.ToBase64String(Encoding.ASCII.GetBytes(Guid.NewGuid().ToString().Substring(0, 16)));
            string expectedAccept =
                Convert.ToBase64String(SHA1.Create().ComputeHash(Encoding.ASCII.GetBytes(secKey + Magic)));

            var headerString =
                $@"GET {uri.PathAndQuery} HTTP/1.1
Host: {uri.Host}
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Version: 13
Sec-WebSocket-Key: {secKey}

";

            var bytes = Encoding.UTF8.GetBytes(headerString);

            await _networkStream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);

            await _networkStream.FlushAsync(cancellationToken);

            Console.WriteLine("before ReadAsync");

            var buffer = new byte[1024];
            var resultLenth = await _networkStream.ReadAsync(buffer, 0, 1024);

            var resultString = new StringReader(Encoding.UTF8.GetString(buffer, 0, resultLenth));

            var RespCode = 0;
            var headers = new Dictionary<string, string>();

            var line = resultString.ReadLine();
            while (line != null)
            {
                Console.WriteLine(line);
                if (line.StartsWith("HTTP/1.1 ") && line.Length > 11)
                    RespCode = Convert.ToInt16(line.Substring(9, 3));
                else
                {
                    var items = line.Split(new[] {':'}, 2);
                    if (items.Length == 2)
                        headers[items[0]] = items[1].TrimStart();
                }

                line = resultString.ReadLine();
            }

            headers.ForEach(x => Console.WriteLine($"{x.Key} == {x.Value}"));

            if (RespCode != (int) HttpStatusCode.SwitchingProtocols)
                throw new WebSocketException("The server returned status code '" + (int) RespCode +
                                             "' when status code '101' was expected");
            if (!string.Equals(headers["Upgrade"], "WebSocket", StringComparison.OrdinalIgnoreCase)
                || !string.Equals(headers["Connection"], "Upgrade", StringComparison.OrdinalIgnoreCase)
                || !string.Equals(headers["Sec-WebSocket-Accept"], expectedAccept))
                throw new WebSocketException("HTTP header error during handshake");

            state = WebSocketState.Open;
        }

        private void EnsureWebSocketConnected()
        {
            if (state < WebSocketState.Open)
                throw new InvalidOperationException("The WebSocket is not connected");
        }

        private void ValidateArraySegment(ArraySegment<byte> segment)
        {
            if (segment.Array == null)
                throw new ArgumentNullException("buffer.Array");
            if (segment.Offset < 0)
                throw new ArgumentOutOfRangeException("buffer.Offset");
            if (segment.Offset + segment.Count > segment.Array.Length)
                throw new ArgumentOutOfRangeException("buffer.Count");
        }

        private void EnsureWebSocketState(params WebSocketState[] validStates)
        {
            foreach (var validState in validStates)
                if (state == validState)
                    return;
            throw new WebSocketException("The WebSocket is in an invalid state ('" + state + "') for this operation. Valid states are: " + string.Join(", ", validStates));
        }

        const int messageTypeText = 1;
        const int messageTypeBinary = 2;
        const int messageTypeClose = 8;

        private WebSocketMessageType WireToMessageType(byte msgType)
        {

            if (msgType == messageTypeText)
                return WebSocketMessageType.Text;
            if (msgType == messageTypeBinary)
                return WebSocketMessageType.Binary;
            return WebSocketMessageType.Close;
        }

        private byte MessageTypeToWire(WebSocketMessageType type)
        {
            if (type == WebSocketMessageType.Text)
                return messageTypeText;
            if (type == WebSocketMessageType.Binary)
                return messageTypeBinary;
            return messageTypeClose;
        }

        private int WriteHeader(WebSocketMessageType type, ArraySegment<byte> buffer, bool endOfMessage)
        {
            var opCode = MessageTypeToWire(type);
            var length = buffer.Count;

            headerBuffer[0] = (byte)(opCode | (endOfMessage ? 0x80 : 0));
            if (length < 126)
            {
                headerBuffer[1] = (byte)length;
            }
            else if (length <= ushort.MaxValue)
            {
                headerBuffer[1] = (byte)126;
                headerBuffer[2] = (byte)(length / 256);
                headerBuffer[3] = (byte)(length % 256);
            }
            else {
                headerBuffer[1] = (byte)127;

                int left = length;
                int unit = 256;

                for (int i = 9; i > 1; i--)
                {
                    headerBuffer[i] = (byte)(left % unit);
                    left = left / unit;
                }
            }

            var l = Math.Max(0, headerBuffer[1] - 125);
            var maskOffset = 2 + l * l * 2;
            GenerateMask(headerBuffer, maskOffset);

            // Since we are client only, we always mask the payload
            headerBuffer[1] |= 0x80;

            return maskOffset;
        }

        private void GenerateMask(byte[] mask, int offset)
        {
            mask[offset + 0] = (byte)random.Next(0, 255);
            mask[offset + 1] = (byte)random.Next(0, 255);
            mask[offset + 2] = (byte)random.Next(0, 255);
            mask[offset + 3] = (byte)random.Next(0, 255);
        }

        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            EnsureWebSocketConnected();
            ValidateArraySegment(arraySegment);
            EnsureWebSocketState(WebSocketState.Open, WebSocketState.CloseSent);

            bool isLast;
            WebSocketMessageType type;
            long length;

            if (remaining == 0)
            {
                // First read the two first bytes to know what we are doing next
                var readLength = await _networkStream.ReadAsync(headerBuffer, 0, 2, token);
                isLast = (headerBuffer[0] >> 7) > 0;
                var isMasked = (headerBuffer[1] >> 7) > 0;
                int mask = 0;
                type = WireToMessageType((byte) (headerBuffer[0] & 0xF));
                length = headerBuffer[1] & 0x7F;
                int offset = 0;
                if (length == 126)
                {
                    offset = 2;
                    readLength = await _networkStream.ReadAsync(headerBuffer, 2, offset, token);
                    length = (headerBuffer[2] << 8) | headerBuffer[3];
                }
                else if (length == 127)
                {
                    offset = 8;
                    readLength = await _networkStream.ReadAsync(headerBuffer, 2, offset, token);
                    length = 0;
                    for (int i = 2; i <= 9; i++)
                        length = (length << 8) | headerBuffer[i];
                }

                if (isMasked)
                {
                    readLength = await _networkStream.ReadAsync(headerBuffer, 2 + offset, 4, token);
                    for (int i = 0; i < 4; i++)
                    {
                        var pos = i + offset + 2;
                        mask = (mask << 8) | headerBuffer[pos];
                    }
                }
            }
            else
            {
                isLast = (headerBuffer[0] >> 7) > 0;
                type = WireToMessageType((byte) (headerBuffer[0] & 0xF));
                length = remaining;
            }

            if (type == WebSocketMessageType.Close)
            {
                state = WebSocketState.Closed;
                var tmpBuffer = new byte[length];
                var readLength = await _networkStream.ReadAsync(tmpBuffer, 0, tmpBuffer.Length, token);
                var closeStatus = (WebSocketCloseStatus) (tmpBuffer[0] << 8 | tmpBuffer[1]);
                var closeDesc = tmpBuffer.Length > 2
                    ? Encoding.UTF8.GetString(tmpBuffer, 2, tmpBuffer.Length - 2)
                    : string.Empty;
                return new WebSocketReceiveResult((int) length, type, isLast, closeStatus, closeDesc);
            }
            else
            {
                var readSoFar = (int) (arraySegment.Count < length ? arraySegment.Count : length);
                var readLength = await _networkStream.ReadAsync(arraySegment.Array, arraySegment.Offset, readSoFar, token);
                remaining = length - readSoFar;

                return new WebSocketReceiveResult((int)readSoFar, type, isLast && remaining == 0);
            }
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus protocolError, string abortingBulkInsertBecauseReceivingUnexpectedResponseFromServerProtocolViolation, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public override async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
            EnsureWebSocketConnected();
            ValidateArraySegment(segment);
            if (_networkStream == null)
                throw new WebSocketException(WebSocketError.Faulted);
            var count = segment.Count + HeaderMaxLength;
            if (sendBuffer == null || sendBuffer.Length < count)
                sendBuffer = new byte[count];
            EnsureWebSocketState(WebSocketState.Open, WebSocketState.CloseReceived);
            var maskOffset = WriteHeader(messageType, segment, endOfMessage);

            var headerLength = maskOffset + 4;
            Array.Copy(headerBuffer, sendBuffer, headerLength);

            if (segment.Count > 0)
            {
                for (var i = 0; i < segment.Count; i++)
                    sendBuffer[i + headerLength] = (byte)(segment.Array[segment.Offset + i] ^ headerBuffer[maskOffset + (i % 4)]);
            }


            await _networkStream.WriteAsync(sendBuffer, 0, segment.Count + headerLength, token);
        }

        public override void Abort()
        {
            throw new NotImplementedException();
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}