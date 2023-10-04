using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace Raven.Server.Utils;

public sealed class BackupStream : Stream
{
    private readonly Stream _inner;
    private readonly byte _b;
    private bool _hasRead;

    public BackupStream(Stream inner, byte b)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _b = b;
    }

    public override void Flush()
    {
        _inner.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            buffer[offset] = _b;
            return 1;
        }

        return _inner.Read(buffer, offset, count);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            buffer[offset] = _b;
            return Task.FromResult(1);
        }

        return _inner.ReadAsync(buffer, offset, count, cancellationToken);
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            buffer.Span[0] = _b;
            return ValueTask.FromResult(1);
        }

        return _inner.ReadAsync(buffer, cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;

    public override long Position
    {
        get
        {
            return _hasRead ? _inner.Position : 0;
        }

        set { throw new NotSupportedException(); }
    }

    public override ValueTask DisposeAsync()
    {
        return _inner.DisposeAsync();
    }

    protected override void Dispose(bool disposing)
    {
        _inner.Dispose();
    }
}
