using System;
using System.IO;

namespace Raven.Server.Utils
{
    public class CombinedReadingStream : Stream
    {
        private readonly Stream[] _streams;
        private readonly long[] _startingPositions;
        private long _position;
        private readonly long _totalLength;
        private int _index;

        public CombinedReadingStream(Stream[] streams)
        {
            _streams = streams;
            _startingPositions = new long[streams.Length];

            _position = 0;
            _index = 0;

            _startingPositions[0] = 0;
            for (var i = 1; i < _startingPositions.Length; i++)
            {
                _startingPositions[i] = _startingPositions[i - 1] + _streams[i - 1].Length;
            }

            _totalLength = _startingPositions[_startingPositions.Length - 1] + _streams[_streams.Length - 1].Length;
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override void Flush()
        {
            throw new NotSupportedException("The method or operation is not supported by CombinedReadingStream.");
        }

        public override long Length => _totalLength;

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                if (value < 0 || value > _totalLength)
                    throw new ArgumentOutOfRangeException("Position");

                _position = value;

                while (_index > 0 && _position < _startingPositions[_index])
                {
                    _streams[_index].Position = 0;
                    _index--;
                }

                while (_index < _streams.Length - 1 && _position >= _startingPositions[_index] + _streams[_index].Length)
                {
                    _streams[_index].Position = _streams[_index].Length;
                    _index++;
                }

                if (_index == 0)
                    _streams[_index].Position = _position;
                else
                    _streams[_index].Position = _position - _startingPositions[_index];
            }
        }

        public override int ReadByte()
        {
            var value = _streams[_index].ReadByte();
            _position++;

            return value;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var result = 0;
            while (count > 0)
            {
                var bytesRead = _streams[_index].Read(buffer, offset, count);

                result += bytesRead;
                offset += bytesRead;
                count -= bytesRead;
                _position += bytesRead;

                if (count > 0)
                {
                    if (_index < _streams.Length - 1)
                        _index++;
                    else
                        break;
                }
            }

            return result;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    break;

                case SeekOrigin.Current:
                    Position += offset;
                    break;

                case SeekOrigin.End:
                    Position = Length + offset;
                    break;
            }

            return Position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException("The method or operation is not supported by CombinedReadingStream.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("The method or operation is not supported by CombinedReadingStream.");
        }
    }
}