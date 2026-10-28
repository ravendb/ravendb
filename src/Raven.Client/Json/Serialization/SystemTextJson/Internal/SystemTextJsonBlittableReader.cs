using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableReader : IJsonReader
    {
        private ContextMemoryBufferWriter _buffer;

        public void Initialize(BlittableJsonReaderObject blittable)
        {
            if (blittable.Modifications != null)
                blittable = blittable._context.ReadObject(blittable, "system-text-json/modifications");

            _buffer = new ContextMemoryBufferWriter(blittable._context, Math.Max(blittable.Size * 2, 512));

            using (var stream = new BufferWriterStream(_buffer))
                blittable.WriteJsonTo(stream);
        }

        public ReadOnlySpan<byte> GetUtf8Json()
        {
            return _buffer.WrittenSpan;
        }

        public void ReturnMemory()
        {
            _buffer?.Dispose();
            _buffer = null;
        }

        public void Dispose()
        {
            ReturnMemory();
        }

        private sealed class BufferWriterStream : Stream
        {
            private readonly ContextMemoryBufferWriter _buffer;

            public BufferWriterStream(ContextMemoryBufferWriter buffer)
            {
                _buffer = buffer;
            }

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length => _buffer.WrittenCount;

            public override long Position
            {
                get => _buffer.WrittenCount;
                set => throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Write(new ReadOnlySpan<byte>(buffer, offset, count));
            }

#if NETSTANDARD2_0
            public void Write(ReadOnlySpan<byte> buffer)
#else
            public override void Write(ReadOnlySpan<byte> buffer)
#endif
            {
                buffer.CopyTo(_buffer.GetSpan(buffer.Length));
                _buffer.Advance(buffer.Length);
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }
        }
    }
}
