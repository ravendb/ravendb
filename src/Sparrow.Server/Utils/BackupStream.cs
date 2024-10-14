using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils;

internal sealed class BackupStream : Stream
{
    private readonly Stream _inner;
    private readonly byte[] _b;
    private bool _hasRead;

    public BackupStream(Stream inner, byte[] b)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _b = b ?? throw new ArgumentNullException(nameof(b));
    }

    public override void Flush()
    {
        throw new NotSupportedException();
    }

    public override int Read(Span<byte> buffer)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            FillBuffer(buffer);
            return _b.Length;
        }

        return _inner.Read(buffer);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            FillBuffer(buffer, offset);
            return _b.Length;
        }

        return _inner.Read(buffer, offset, count);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_hasRead == false)
        {
            _hasRead = true;
            FillBuffer(buffer, offset);
            return Task.FromResult(_b.Length);
        }

        return _inner.ReadAsync(buffer, offset, count, cancellationToken);
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_hasRead == false)
        {
            _hasRead = true;

            FillBuffer(buffer.Span);
            return ValueTask.FromResult(_b.Length);
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

    private void FillBuffer(Span<byte> buffer)
    {
        if (buffer.Length < _b.Length)
            throw new ArgumentException();

        for (var i = 0; i < _b.Length; i++)
            buffer[i] = _b[i];
    }

    private void FillBuffer(byte[] buffer, int offset)
    {
        FillBuffer(new Span<byte>(buffer)[offset..]);
    }
}
