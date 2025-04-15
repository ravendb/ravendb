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

    public override void Flush() => throw new NotImplementedException();

    public override int Read(byte[] buffer, int offset, int count)  => throw new NotImplementedException();

    public override long Seek(long offset, SeekOrigin origin)  => throw new NotImplementedException();

    public override void SetLength(long value) => throw new NotImplementedException();

    public override void Write(byte[] buffer, int offset, int count) => _errorBuffer.AppendUtf8(buffer.AsSpan(offset, count));

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotImplementedException();
    public override long Position { get  => throw new NotImplementedException(); set  => throw new NotImplementedException(); }
}
