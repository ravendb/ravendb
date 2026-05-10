using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI;

/// <summary>
/// A write-only <see cref="Stream"/> wrapper that forwards every write to two underlying streams
/// simultaneously — named after the Unix <c>tee</c> command.
/// The caller owns both inner streams; <see cref="Dispose"/> is intentionally a no-op.
/// </summary>
internal sealed class TeeStream : Stream
{
    private readonly Stream _primary;
    private readonly Stream _secondary;

    public TeeStream(Stream primary, Stream secondary)
    {
        _primary = primary ?? throw new ArgumentNullException(nameof(primary));
        _secondary = secondary ?? throw new ArgumentNullException(nameof(secondary));
    }

    public override bool CanWrite => true;
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _primary.Write(buffer, offset, count);
        _secondary.Write(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _primary.Write(buffer);
        _secondary.Write(buffer);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _primary.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        await _secondary.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _primary.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        await _secondary.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public override void Flush()
    {
        _primary.Flush();
        _secondary.Flush();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _primary.FlushAsync(cancellationToken).ConfigureAwait(false);
        await _secondary.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing) { }
}
