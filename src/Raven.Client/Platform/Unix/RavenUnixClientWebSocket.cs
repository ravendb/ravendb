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

namespace Raven.Client.Platform.Unix
{
    public class RavenUnixClientWebSocket : WebSocket, IDisposable
    {
        private const int MessageTypeText = 1;
        private const int MessageTypeBinary = 2;
        private const int MessageTypeClose = 8;
        private const string Magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        private WebSocketState _state;
        private readonly Random _random = new Random();
        private const int HeaderMaxLength = 14;
        private readonly byte[] _headerBuffer;
        private byte[] _sendBuffer;
        private long _remaining;
        private readonly TcpClient _connection;
        private NetworkStream _networkStream;
        private bool _disposed;

        public override WebSocketState State => _state;

        public RavenUnixClientWebSocket()
        {
            _connection = new TcpClient
            {
                NoDelay = true
            };
            _state = WebSocketState.None;
            _headerBuffer = new byte[HeaderMaxLength];
        }

        public async Task ConnectAsync(Uri uri, CancellationToken cancellationToken)
        {
            _state = WebSocketState.Connecting;
            if (uri.Scheme == "wss")
                throw new NotSupportedException("https is not supported");

            var port = uri.Port == 0 ? 80 : uri.Port;

            await _connection.ConnectAsync(uri.Host, port);

            _networkStream = _connection.GetStream();
            var secKey = Convert.ToBase64String(Encoding.ASCII.GetBytes(Guid.NewGuid().ToString().Substring(0, 16)));
            string expectedAccept = Convert.ToBase64String(SHA1.Create().ComputeHash(Encoding.ASCII.GetBytes(secKey + Magic)));

            var headerString =
                $"GET {uri.PathAndQuery} HTTP/1.1\r\n" +
                $"Host: {uri.Host}\r\n" +
                "Connection: Upgrade\r\n" +
                "Upgrade: websocket\r\n" +
                "Sec-WebSocket-Version: 13\r\n" +
                $"Sec-WebSocket-Key: {secKey}\r\n\r\n";

            var bytes = Encoding.UTF8.GetBytes(headerString);
            await _networkStream.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
            await _networkStream.FlushAsync(cancellationToken);

            var buffer = new byte[1024];
            var resultLenth = await _networkStream.ReadAsync(buffer, 0, 1024, cancellationToken);
            var resultString = new StringReader(Encoding.UTF8.GetString(buffer, 0, resultLenth));

            var respCode = 0;
            var headers = new Dictionary<string, string>();
            var line = resultString.ReadLine();
            while (line != null)
            {
                if (line.StartsWith("HTTP/1.1 ") && line.Length > 11)
                    respCode = Convert.ToInt16(line.Substring(9, 3));
                else
                {
                    var items = line.Split(new[] { ':' }, 2);
                    if (items.Length == 2)
                        headers[items[0]] = items[1].TrimStart();
                }

                line = resultString.ReadLine();
            }

            if (respCode != (int)HttpStatusCode.SwitchingProtocols)
                throw new WebSocketException("The server returned status code '" + (int)respCode +
                                             "' when status code '101' was expected");
            if (!string.Equals(headers["Upgrade"], "WebSocket", StringComparison.OrdinalIgnoreCase)
                || !string.Equals(headers["Connection"], "Upgrade", StringComparison.OrdinalIgnoreCase)
                || !string.Equals(headers["Sec-WebSocket-Accept"], expectedAccept))
                throw new WebSocketException("HTTP header error during handshake");

            _state = WebSocketState.Open;
        }

        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> arraySegment, CancellationToken token)
        {
            EnsureWebSocketConnected();
            ValidateArraySegment(arraySegment);
            EnsureWebSocketState(WebSocketState.Open, WebSocketState.CloseSent);

            bool isLast;
            WebSocketMessageType type;
            long length;

            if (_remaining == 0)
            {
                // First read the two first bytes to know what we are doing next
                await _networkStream.ReadAsync(_headerBuffer, 0, 2, token);
                isLast = (_headerBuffer[0] >> 7) > 0;
                var isMasked = (_headerBuffer[1] >> 7) > 0;
                int mask = 0;
                type = WireToMessageType((byte)(_headerBuffer[0] & 0xF));
                length = _headerBuffer[1] & 0x7F;
                int offset = 0;
                if (length == 126)
                {
                    offset = 2;
                    await _networkStream.ReadAsync(_headerBuffer, 2, offset, token);
                    length = (_headerBuffer[2] << 8) | _headerBuffer[3];
                }
                else if (length == 127)
                {
                    offset = 8;
                    await _networkStream.ReadAsync(_headerBuffer, 2, offset, token);
                    length = 0;
                    for (int i = 2; i <= 9; i++)
                        length = (length << 8) | _headerBuffer[i];
                }

                if (isMasked)
                {
                    await _networkStream.ReadAsync(_headerBuffer, 2 + offset, 4, token);
                    for (int i = 0; i < 4; i++)
                    {
                        var pos = i + offset + 2;
                        mask = (mask << 8) | _headerBuffer[pos];
                    }
                }
            }
            else
            {
                isLast = (_headerBuffer[0] >> 7) > 0;
                type = WireToMessageType((byte)(_headerBuffer[0] & 0xF));
                length = _remaining;
            }

            if (type == WebSocketMessageType.Close)
            {
                _state = WebSocketState.Closed;
                var tmpBuffer = new byte[length];
                var readLength = await _networkStream.ReadAsync(tmpBuffer, 0, tmpBuffer.Length, token);
                var closeStatus = (WebSocketCloseStatus)(tmpBuffer[0] << 8 | tmpBuffer[1]);
                var closeDesc = tmpBuffer.Length > 2
                    ? Encoding.UTF8.GetString(tmpBuffer, 2, tmpBuffer.Length - 2)
                    : string.Empty;
                return new WebSocketReceiveResult((int)length, type, isLast, closeStatus, closeDesc);
            }
            else
            {
                var readSoFar = (int)(arraySegment.Count < length ? arraySegment.Count : length);
                var readLength = await _networkStream.ReadAsync(arraySegment.Array, arraySegment.Offset, readSoFar, token);
                _remaining = length - readSoFar;

                return new WebSocketReceiveResult((int)readSoFar, type, isLast && _remaining == 0);
            }
        }

        public override async Task SendAsync(ArraySegment<byte> segment, WebSocketMessageType messageType, bool endOfMessage, CancellationToken token)
        {
            EnsureWebSocketConnected();
            ValidateArraySegment(segment);
            if (_networkStream == null)
                throw new WebSocketException(WebSocketError.Faulted);
            var count = segment.Count + HeaderMaxLength;
            if (_sendBuffer == null || _sendBuffer.Length < count)
                _sendBuffer = new byte[count];
            EnsureWebSocketState(WebSocketState.Open, WebSocketState.CloseReceived);
            var maskOffset = WriteHeader(messageType, segment, endOfMessage);

            var headerLength = maskOffset + 4;
            Array.Copy(_headerBuffer, _sendBuffer, headerLength);

            if (segment.Count > 0)
            {
                for (var i = 0; i < segment.Count; i++)
                    _sendBuffer[i + headerLength] = (byte)(segment.Array[segment.Offset + i] ^ _headerBuffer[maskOffset + (i % 4)]);
            }

            await _networkStream.WriteAsync(_sendBuffer, 0, segment.Count + headerLength, token);
        }

        private void EnsureWebSocketConnected()
        {
            if (_state < WebSocketState.Open)
                throw new InvalidOperationException("The WebSocket is not connected");
        }

        public override WebSocketCloseStatus? CloseStatus
        {
            get
            {
                if (_state != WebSocketState.Closed)
                    return null;
                return WebSocketCloseStatus.Empty;
            }
        }

        private static void ValidateArraySegment(ArraySegment<byte> segment)
        {
            if (segment.Array == null)
                throw new ArgumentNullException("segment.Array");
            if (segment.Offset < 0)
                throw new ArgumentOutOfRangeException("segment.Offset");
            if (segment.Offset + segment.Count > segment.Array.Length)
                throw new ArgumentOutOfRangeException("segment.Count");
        }

        private void EnsureWebSocketState(params WebSocketState[] validStates)
        {
            foreach (var validState in validStates)
                if (_state == validState)
                    return;
            throw new WebSocketException("The WebSocket is in an invalid state ('" + _state + "') for this operation. Valid states are: " + string.Join(", ", validStates));
        }

        private static WebSocketMessageType WireToMessageType(byte msgType)
        {
            switch (msgType)
            {
                case MessageTypeText:
                    return WebSocketMessageType.Text;
                case MessageTypeBinary:
                    return WebSocketMessageType.Binary;
            }
            return WebSocketMessageType.Close;
        }

        private static byte MessageTypeToWire(WebSocketMessageType type)
        {
            switch (type)
            {
                case WebSocketMessageType.Text:
                    return MessageTypeText;
                case WebSocketMessageType.Binary:
                    return MessageTypeBinary;
            }
            return MessageTypeClose;
        }

        private int WriteHeader(WebSocketMessageType type, ArraySegment<byte> buffer, bool endOfMessage)
        {
            var opCode = MessageTypeToWire(type);
            var length = buffer.Count;

            _headerBuffer[0] = (byte)(opCode | (endOfMessage ? 0x80 : 0));
            if (length < 126)
            {
                _headerBuffer[1] = (byte)length;
            }
            else if (length <= ushort.MaxValue)
            {
                _headerBuffer[1] = (byte)126;
                _headerBuffer[2] = (byte)(length / 256);
                _headerBuffer[3] = (byte)(length % 256);
            }
            else {
                _headerBuffer[1] = (byte)127;

                var left = length;
                const int unit = 256;

                for (var i = 9; i > 1; i--)
                {
                    _headerBuffer[i] = (byte)(left % unit);
                    left = left / unit;
                }
            }

            var l = Math.Max(0, _headerBuffer[1] - 125);
            var maskOffset = 2 + l * l * 2;
            GenerateMask(_headerBuffer, maskOffset);

            // Since we are client only, we always mask the payload
            _headerBuffer[1] |= 0x80;

            return maskOffset;
        }

        private void GenerateMask(byte[] mask, int offset)
        {
            mask[offset + 0] = (byte)_random.Next(0, 255);
            mask[offset + 1] = (byte)_random.Next(0, 255);
            mask[offset + 2] = (byte)_random.Next(0, 255);
            mask[offset + 3] = (byte)_random.Next(0, 255);
        }

        public override async Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken token)
        {
            EnsureWebSocketConnected();
            await _networkStream.FlushAsync(token);
            await SendCloseFrame(closeStatus, statusDescription, token).ConfigureAwait(false);
            _state = WebSocketState.CloseSent;
        }

        private async Task SendCloseFrame(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            var statusDescBuffer = string.IsNullOrEmpty(statusDescription) ? new byte[2] : new byte[2 + Encoding.UTF8.GetByteCount(statusDescription)];
            statusDescBuffer[0] = (byte)(((ushort)closeStatus) >> 8);
            statusDescBuffer[1] = (byte)(((ushort)closeStatus) & 0xFF);
            if (!string.IsNullOrEmpty(statusDescription))
                Encoding.UTF8.GetBytes(statusDescription, 0, statusDescription.Length, statusDescBuffer, 2);
            await SendAsync(new ArraySegment<byte>(statusDescBuffer), WebSocketMessageType.Close, true, cancellationToken).ConfigureAwait(false);
        }

        public override void Abort()
        {
            throw new NotImplementedException();
        }

        public override async Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
        {
            EnsureWebSocketConnected();
            await SendCloseFrame(closeStatus, statusDescription, cancellationToken).ConfigureAwait(false);
            _state = WebSocketState.CloseSent;
            await ReceiveAsync(new ArraySegment<byte>(new byte[0]), cancellationToken).ConfigureAwait(false);
            _state = WebSocketState.Closed;
        }

        public override string CloseStatusDescription => null;

        public override string SubProtocol => null; // Not Implemented

        public override void Dispose()
        {
            if (!_disposed)
                _disposed = true;
            _connection?.Dispose();
        }
    }
}