using System;
using Sparrow.Global;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Voron.Impl;

public unsafe class StreamBufferAllocator : ILowMemoryHandler
{
    public static readonly StreamBufferAllocator Instance = new StreamBufferAllocator();

    private readonly PerCoreContainer<Buffer> _buffers = new PerCoreContainer<Buffer>(8);
    private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();

    private static readonly int BufferSize = PlatformDetails.Is32Bits == false
        ? 512 * Constants.Size.Kilobyte
        : 16 * Constants.Size.Kilobyte;

    private StreamBufferAllocator()
    {
        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public Buffer Rent()
    {
        if (_buffers.TryPull(out var buffer))
            return buffer;

        var ptr = NativeMemory.AllocateMemory(BufferSize);
        return new Buffer(ptr, BufferSize);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        if (lowMemorySeverity != LowMemorySeverity.ExtremelyLow)
            return;

        if (_isExtremelyLowMemory.Raise() == false)
            return;

        foreach (var buffer in _buffers.EnumerateAndClear())
        {
            buffer.Free();
        }
    }

    public void LowMemoryOver()
    {
        _isExtremelyLowMemory.Lower();
    }

    public class Buffer : IDisposable
    {
        private readonly byte* _ptr;
        private readonly long _size;

        public byte* Pointer => _ptr;

        public static readonly Buffer Null = new Buffer(null, 0);

        public Span<byte> AsSpan() => new Span<byte>(_ptr, (int)_size);

        public Buffer(byte* ptr, long size)
        {
            _ptr = ptr;
            _size = size;
        }

        public void Free()
        {
            NativeMemory.Free(_ptr, _size);
        }

        public void Dispose()
        {
            if (_ptr != null && Instance._buffers.TryPush(this) == false)
                Free();
        }
    }
}
