using System;
using System.IO;

namespace Raven.Server.SchemaValidation.ErrorMessage;

public class ErrorBufferStreamWrapper : Stream
{
    private readonly IErrorBuffer _errorBuffer;

    public ErrorBufferStreamWrapper(IErrorBuffer errorBuffer)
    {
        _errorBuffer = errorBuffer;
    }

    public override void Flush() {}

    public override int Read(byte[] buffer, int offset, int count)  => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin)  => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => _errorBuffer.AppendUtf8(buffer.AsSpan(offset, count));

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get  => throw new NotSupportedException(); set  => throw new NotSupportedException(); }
}
