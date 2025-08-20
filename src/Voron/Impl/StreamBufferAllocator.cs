using System;
using System.Threading;
using Sparrow;
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

    private readonly BufferStats _stats = new BufferStats();
    private readonly PerCoreContainer<Buffer, BufferTrackingPolicy> _buffers;
    private readonly MultipleUseFlag _isExtremelyLowMemory = new MultipleUseFlag();

    private static readonly int BufferSize = PlatformDetails.Is32Bits == false
        ? 512 * Constants.Size.Kilobyte
        : 16 * Constants.Size.Kilobyte;

    private StreamBufferAllocator()
    {
        var policy = new BufferTrackingPolicy(_stats, BufferSize);
        _buffers = new PerCoreContainer<Buffer, BufferTrackingPolicy>(
            numberOfGlobalSlots: 8,
            numberOfSlotsPerCore: 8,
            policy: policy);
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

        _buffers.Cleanup(new RemoveAllPolicy<Buffer>(), x => x.Free());
    }

    public void LowMemoryOver()
    {
        _isExtremelyLowMemory.Lower();
    }

    public StreamBufferStats GetStats()
    {
        return new StreamBufferStats
        {
            BufferSize = BufferSize,
            TotalPoolSize = Volatile.Read(ref _stats.Bytes),
            TotalNumberOfItems = (int)Volatile.Read(ref _stats.Count)
        };
    }

    private sealed class BufferStats
    {
        public long Count;
        public long Bytes;
    }

    private readonly struct BufferTrackingPolicy : IPerCoreContainerPolicy<Buffer>
    {
        private readonly BufferStats _stats;
        private readonly long _bufferSize;

        public BufferTrackingPolicy(BufferStats stats, long bufferSize)
        {
            _stats = stats;
            _bufferSize = bufferSize;
        }

        public bool CanRemove => false;

        public bool ShouldRemove(Buffer item, int coreIndex) => false;

        public void OnAdded(Buffer item, int coreIndex)
        {
            Interlocked.Increment(ref _stats.Count);
            Interlocked.Add(ref _stats.Bytes, _bufferSize);
        }

        public void OnRemoved(Buffer item, int coreIndex)
        {
            Interlocked.Decrement(ref _stats.Count);
            Interlocked.Add(ref _stats.Bytes, -_bufferSize);
        }
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

public sealed class StreamBufferStats
{
    public long BufferSize { get; set; }
    public long TotalPoolSize { get; set; }
    public int TotalNumberOfItems { get; set; }

    public Size BufferSizeHumane => new Size(BufferSize, SizeUnit.Bytes);
    public Size TotalPoolSizeHumane => new Size(TotalPoolSize, SizeUnit.Bytes);
}
