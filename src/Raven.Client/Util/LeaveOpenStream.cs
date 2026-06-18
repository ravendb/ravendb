using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
    // Delegates all operations to the inner stream but intentionally does not dispose it,
    // leaving ownership of the inner stream's lifetime with the caller.
    // Useful when handing a stream to an API that would otherwise take ownership and dispose it
    // (e.g. StreamContent or a compression stream) and that disposal is not desired.
    internal sealed class LeaveOpenStream : Stream
    {
        private readonly Stream _inner;

        public LeaveOpenStream(Stream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => _inner.CanWrite;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Flush() => _inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.ReadAsync(buffer, offset, count, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value) => _inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);

#if !NETSTANDARD2_0
        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);

        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

        public override ValueTask DisposeAsync() => default;
#endif

        // Intentionally does not dispose the inner stream - its lifetime is owned by the caller, not by this wrapper.
        protected override void Dispose(bool disposing)
        {
        }
    }
}
