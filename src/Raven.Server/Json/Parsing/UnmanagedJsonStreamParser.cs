using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Server.Json.Parsing
{
    public class UnmanagedJsonStreamParser: UnmanagedJsonParserAbstract
    {
        private readonly Stream _stream;
        public UnmanagedJsonStreamParser(Stream stream, RavenOperationContext ctx, JsonParserState state, string documentId) : base(ctx, state, documentId)
        {
            _stream = stream;
        }

        public override async Task LoadBufferFromSource()
        {
            _currentStrStart = 0;
            _pos = 0;
            _bufSize = 0;
            var read = await _stream.ReadAsync(_buffer, _bufSize, _buffer.Length - _bufSize);
            if (read == 0)
                throw new EndOfStreamException();
            _bufSize += read;
        }

        public override async Task EnsureRestOfToken(byte[] expectedBuffer, string expected)
        {
            var size = expectedBuffer.Length - 1;
            while (_pos + size >= _bufSize)// end of buffer, need to read more bytes
            {
                var lenToMove = _bufSize - _pos;
                //TODO: memmove ?
                for (int i = 0; i < lenToMove; i++)
                {
                    _buffer[i] = _buffer[i + _pos];
                }
                _bufSize = await _stream.ReadAsync(_buffer, lenToMove, _bufSize - lenToMove);
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
