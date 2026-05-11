using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.AI;

internal sealed class TeeStream : Stream
{
    private readonly Stream _primary;
    private readonly Stream _ownedSecondary;
    private bool _disposed;

    public TeeStream(Stream primary, Stream ownedSecondary)
    {
        _primary = primary ?? throw new ArgumentNullException(nameof(primary));
        _ownedSecondary = ownedSecondary ?? throw new ArgumentNullException(nameof(ownedSecondary));
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
        _ownedSecondary.Write(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _primary.Write(buffer);
        _ownedSecondary.Write(buffer);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await _primary.WriteAsync(buffer, offset, count, cancellationToken);
        await _ownedSecondary.WriteAsync(buffer, offset, count, cancellationToken);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _primary.WriteAsync(buffer, cancellationToken);
        await _ownedSecondary.WriteAsync(buffer, cancellationToken);
    }

    public override void Flush()
    {
        _primary.Flush();
        _ownedSecondary.Flush();
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _primary.FlushAsync(cancellationToken);
        await _ownedSecondary.FlushAsync(cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        try
        {
            if (disposing)
            {
                // Primary stream is NOT owned — do not dispose.
                _ownedSecondary.Dispose();
            }
        }
        finally
        {
            _disposed = true;
            base.Dispose(disposing);
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            // Primary stream is NOT owned — do not dispose.
            await _ownedSecondary.DisposeAsync();
        }
        finally
        {
            _disposed = true;
            await base.DisposeAsync();
        }
    }
}
