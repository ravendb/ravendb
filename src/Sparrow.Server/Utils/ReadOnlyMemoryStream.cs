using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparrow.Server.Utils;

public class ReadOnlyMemoryStream<T> : Stream
    where T : unmanaged
{
    public override bool CanSeek => false;
    public override bool CanRead => true;
    public override bool CanWrite => false;
    public override long Length => _length;
    public override long Position { get; set; }
    private readonly ReadOnlyMemory<T> _memory;
    private readonly int _length;

    public ReadOnlyMemoryStream(ReadOnlyMemory<T> memory, int? lengthInBytes = null)
    {
        _memory = memory;
        _length = lengthInBytes ?? Unsafe.SizeOf<T>() * memory.Length;
    }
    
    public override int Read(byte[] buffer, int offset, int count)
    {
        var left = Length - Position;
        var readLimit = (int)Math.Min(count, left);
        var memoryAsByteSpan = MemoryMarshal.Cast<T, byte>(_memory.Span);
        memoryAsByteSpan.Slice((int)Position, readLimit).CopyTo(buffer.AsSpan(start: offset));
        Position += readLimit;
        return readLimit;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException($"{nameof(ReadOnlyMemoryStream<T>)} is read only. {nameof(Seek)} is not supported.");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException($"{nameof(ReadOnlyMemoryStream<T>)} is read only. {nameof(SetLength)} is not supported.");
    }
    
    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException($"{nameof(ReadOnlyMemoryStream<T>)} is read only. {nameof(Write)} is not supported.");
    }
    
    public override void Flush()
    {
        throw new NotSupportedException($"{nameof(ReadOnlyMemoryStream<T>)} is read only. {nameof(Flush)} is not supported.");
    }
}
