using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Json.Parsing
{
    public class UnmanagedJsonWebSocketParser : UnmanagedJsonParserAbstract
    {
        private readonly WebSocket _webSocket;

        public UnmanagedJsonWebSocketParser(WebSocket webSocket, RavenOperationContext ctx, JsonParserState state, string documentId) : base(ctx, state, documentId)
        {
            _webSocket = webSocket;
        }


        public override async Task LoadBufferFromSource()
        {
            _currentStrStart = 0;
            _pos = 0;
            _bufSize = 0;
            var arrayBuffer = new ArraySegment<byte>(_buffer,_bufSize, _buffer.Length - _bufSize);
            var websocketResult = await _webSocket.ReceiveAsync(arrayBuffer, CancellationToken.None);
            
            if (websocketResult.Count == 0)
                throw new EndOfStreamException();
            _bufSize += websocketResult.Count;
        }

        public override async Task EnsureRestOfToken(byte[] expectedBuffer, string expected)
        {
            var size = expectedBuffer.Length - 1;

            while (_pos + size >= _bufSize)// end of buffer, need to read more bytes
            {
                var lenToMove = _bufSize - _pos;
                for (int i = 0; i < lenToMove; i++)
                {
                    _buffer[i] = _buffer[i + _pos];
                }

                var arrayBuffer = new ArraySegment<byte>(_buffer, lenToMove, _bufSize - lenToMove);
                var websocketResult = await _webSocket.ReceiveAsync(arrayBuffer, CancellationToken.None);
                _bufSize = websocketResult.Count;

                if (_bufSize == 0)
                    throw new EndOfStreamException();
                _bufSize += lenToMove;
                _pos = 0;
            }
            for (int i = 0; i < size; i++)
            {
                if (_buffer[_pos++] != expectedBuffer[i + 1])
                    throw CreateException("Invalid token found, expected: " + expected);
            }
        }
    }
}
