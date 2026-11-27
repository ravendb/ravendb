using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableReader : IJsonReader
    {
        [ThreadStatic]
        private static JsonOperationContext _current;

        private ContextMemoryBufferWriter _buffer;

        private JsonOperationContext _previous;

        private bool _entered;

        internal static JsonOperationContext Current => _current;

        public void Initialize(BlittableJsonReaderObject blittable)
        {
            if (_entered == false)
            {
                _previous = _current;
                _entered = true;
            }

            _current = blittable._context;

            BlittableJsonReaderObject materialized = blittable.Modifications != null
                ? blittable._context.ReadObject(blittable, "system-text-json/modifications")
                : null;

            using (materialized)
            {
                BlittableJsonReaderObject source = materialized ?? blittable;
                _buffer = new ContextMemoryBufferWriter(source._context, Math.Max(source.Size * 2, 512));

                using (var stream = new BufferWriterStream(_buffer))
                using (var writer = new BlittableJsonTextWriter(source._context, stream) { EscapeNamedFloatingPointLiterals = true })
                    source.WriteJsonTo(writer);
            }
        }

        public ReadOnlySpan<byte> GetUtf8Json()
        {
            return _buffer.WrittenSpan;
        }

        public void ReturnMemory()
        {
            _buffer?.Dispose();
            _buffer = null;

            if (_entered)
            {
                _current = _previous;
                _previous = null;
                _entered = false;
            }
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
