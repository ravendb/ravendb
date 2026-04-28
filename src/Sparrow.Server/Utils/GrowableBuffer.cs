using System;
using Sparrow.Platform;

namespace Sparrow.Server.Utils;

public unsafe interface IBufferGrowth
{
    public static readonly int MaxBufferSizeInBytes = int.MaxValue - sizeof(ByteStringStorage);

    public int GetInitialSize(in long initialSize);
    public int GetNewSize(in int currentSizeInBytes);
    public bool GrowingThresholdExceed(in int count, in int sizeInBytes);
}

public readonly struct Progressive : IBufferGrowth
{
    public int GetNewSize(in int currentSizeInBytes)
    {
        // Slower growth on 32-bit platforms
        float platformScalar = PlatformDetails.Is32Bits ? 1.1f : 1.5f;

        long size = currentSizeInBytes > 16 * Sparrow.Global.Constants.Size.Megabyte
            ? (long)(currentSizeInBytes * platformScalar)
            : (long)currentSizeInBytes * 2;

        if (size > IBufferGrowth.MaxBufferSizeInBytes)
            size = IBufferGrowth.MaxBufferSizeInBytes;

        int truncated = (int)size;

        // Represent array as N*sizeof(long)
        return truncated - (truncated % sizeof(long));
    }

    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        // 1/16 left
        var amountOfLongs = (sizeInBytes / sizeof(long));
        return (amountOfLongs - count) < Math.Max(1, amountOfLongs / 16);
    }

    public int GetInitialSize(in long initialSize)
    {
        long suggested = 4 * Math.Min(Math.Max(Sparrow.Global.Constants.Size.Kilobyte, initialSize), 16 * Sparrow.Global.Constants.Size.Kilobyte);
        long size = Math.Max(suggested, initialSize);
        if (size > IBufferGrowth.MaxBufferSizeInBytes)
            size = IBufferGrowth.MaxBufferSizeInBytes;

        int truncated = (int)size;

        // Represent array as N*sizeof(long)
        return truncated - (truncated % sizeof(long));
    }
}

public unsafe struct GrowableBuffer<TGrowth> : IDisposable
    where TGrowth : IBufferGrowth
{
    private readonly TGrowth _growthCalculator = default;
    private ByteStringContext _context;
    private ByteString _buffer;
    private int _count;
    private int _maxAllocationInBytes;
    public int Count => _count;
    public bool IsInitialized;

    public Span<long> GetSpace()
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        var space = _buffer.ToSpan<long>().Slice(_count);
        if (space.IsEmpty)
            ThrowCannotGrowAnyFurther();
        return space;
    }

    public bool TryGetSpace(out Span<long> space)
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        space = _buffer.ToSpan<long>().Slice(_count);
        return space.IsEmpty == false;
    }

    private void ThrowCannotGrowAnyFurther()
    {
        throw new InvalidOperationException(
            $"GrowableBuffer cannot allocate more space. The buffer is full ({_count} items) and has reached its maximum allocation of {_maxAllocationInBytes} bytes.");
    }

    public Span<long> Results => _buffer.ToSpan<long>().Slice(0, _count);

    public bool HasEmptySpace => _buffer.Length == (_count * sizeof(long));

    public int AllocatedSizeInBytes => _buffer.Length;

    public int MaxAllocationInBytes => _maxAllocationInBytes;

    public GrowableBuffer()
    {
    }

    public void AddUsage(in int count) => _count += count;

    public void Truncate(in int newCount) => _count = newCount;

    public void Init(ByteStringContext context, in long initialSize, long maxAllocationInBytes = long.MaxValue)
    {
        if (TryInit(context, initialSize, maxAllocationInBytes) == false)
            ThrowFailedToInitialize(initialSize, maxAllocationInBytes);
    }

    public bool TryInit(ByteStringContext context, in long initialSize, long maxAllocationInBytes = long.MaxValue)
    {
        _context = context;

        long clampedMax = Math.Min(Math.Max(0, maxAllocationInBytes), IBufferGrowth.MaxBufferSizeInBytes);
        _maxAllocationInBytes = (int)(clampedMax - (clampedMax % sizeof(long)));

        int initial;
        if (initialSize <= 0)
        {
            initial = _growthCalculator.GetInitialSize(0);
        }
        else
        {
            long requested = initialSize >= IBufferGrowth.MaxBufferSizeInBytes / sizeof(long)
                ? IBufferGrowth.MaxBufferSizeInBytes
                : initialSize * sizeof(long);
            initial = (int)(requested - (requested % sizeof(long)));
        }

        if (initial > _maxAllocationInBytes)
            return false;

        _context.Allocate(initial, out _buffer);
        IsInitialized = true;
        return true;
    }

    private static void ThrowFailedToInitialize(long initialSize, long maxAllocationInBytes) =>
        throw new InvalidOperationException($"GrowableBuffer cannot allocate the requested buffer: initialSize={initialSize} items would exceed maxAllocationInBytes={maxAllocationInBytes}. You might want to increase `Indexing.Corax.MaxMemoizationSizeInMb`.");

    private void Grow()
    {
        var newSize = _growthCalculator.GetNewSize(_buffer.Length);
        if (newSize > _maxAllocationInBytes)
            newSize = _maxAllocationInBytes;

        if (newSize <= _buffer.Length)
            return;

        _context.Allocate(newSize, out ByteString newBuffer);
        new Span<long>(_buffer.Ptr, _count).CopyTo(new Span<long>(newBuffer.Ptr, _count));
        _context.Release(ref _buffer);
        _buffer = newBuffer;
    }

    public void Dispose()
    {
        if (_buffer.HasValue)
            _context.Release(ref _buffer);
        _buffer = default;
        IsInitialized = false;
    }
}
