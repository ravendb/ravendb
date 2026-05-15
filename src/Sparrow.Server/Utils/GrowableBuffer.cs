using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Platform;

namespace Sparrow.Server.Utils;

public unsafe interface IBufferGrowth
{
    public static readonly int MaxBufferSizeInBytes = int.MaxValue - sizeof(ByteStringStorage);

    public int GetInitialSize(in long initialSize);
    public int GetNewSize(in int currentSizeInBytes);
    public bool GrowingThresholdExceed(in int count, in int sizeInBytes);
}

public readonly struct Constant<TNumber> : IBufferGrowth
    where TNumber : unmanaged, INumber<TNumber>
{
    public int GetInitialSize(in long initialSize)
    {
        return (int)initialSize * Unsafe.SizeOf<TNumber>();
    }

    public int GetNewSize(in int currentSizeInBytes) => currentSizeInBytes * 2;
    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        var amountOfLongs = (sizeInBytes / Unsafe.SizeOf<TNumber>());
        return (amountOfLongs - count) < amountOfLongs / 16;
    }
}

public readonly struct Progressive<TNumber> : IBufferGrowth
where TNumber : unmanaged, INumber<TNumber>
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
        return truncated - (truncated % Unsafe.SizeOf<TNumber>());
    }

    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        // 1/16 left
        var amountOfLongs = (sizeInBytes / Unsafe.SizeOf<TNumber>());
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
        return truncated - (truncated % Unsafe.SizeOf<TNumber>());
    }
}

public unsafe struct GrowableBuffer<TNumber, TGrowth> : IDisposable
    where TGrowth : IBufferGrowth
    where TNumber : unmanaged, INumber<TNumber>
{
    private readonly TGrowth _growthCalculator = default;
    private ByteStringContext _context;
    private ByteString _buffer;
    private int _count;
    private int _maxAllocationInBytes;
    public int Count => _count;
    public bool IsInitialized;
    
    public int Capacity => IsInitialized ? _buffer.Length / sizeof(TNumber) : 0;

    public Span<TNumber> GetSpace()
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        var space = _buffer.ToSpan<TNumber>().Slice(_count);
        if (space.IsEmpty)
            ThrowCannotGrowAnyFurther();
        return space;
    }

    public bool TryGetSpace(out Span<TNumber> space)
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        space = _buffer.ToSpan<TNumber>().Slice(_count);
        return space.IsEmpty == false;
    }

    private void ThrowCannotGrowAnyFurther()
    {
        throw new InvalidOperationException(
            $"GrowableBuffer cannot allocate more space. The buffer is full ({_count} items) and has reached its maximum allocation of {_maxAllocationInBytes} bytes.");
    }

    public Span<TNumber> Results => _buffer.ToSpan<TNumber>().Slice(0, _count);
    
    public bool HasEmptySpace => _buffer.Length == (_count * Unsafe.SizeOf<TNumber>());

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
        _maxAllocationInBytes = (int)(clampedMax - (clampedMax % Unsafe.SizeOf<TNumber>()));

        int initial;
        if (initialSize <= 0)
        {
            initial = _growthCalculator.GetInitialSize(0);
        }
        else
        {
            long requested = initialSize >= IBufferGrowth.MaxBufferSizeInBytes / Unsafe.SizeOf<TNumber>()
                ? IBufferGrowth.MaxBufferSizeInBytes
                : initialSize * Unsafe.SizeOf<TNumber>();
            initial = (int)(requested - (requested % Unsafe.SizeOf<TNumber>()));
        }

        if (initial > _maxAllocationInBytes)
            return false;

        _context.Allocate(initial, out _buffer);
        IsInitialized = true;
        return true;
    }

    private static void ThrowFailedToInitialize(long initialSize, long maxAllocationInBytes) =>
        throw new InvalidOperationException($"GrowableBuffer cannot allocate the requested buffer: initialSize={initialSize} items would exceed the configured maximum allocation of {maxAllocationInBytes} bytes.");

    private void Grow()
    {
        var newSize = _growthCalculator.GetNewSize(_buffer.Length);
        if (newSize > _maxAllocationInBytes)
            newSize = _maxAllocationInBytes;

        if (newSize <= _buffer.Length)
            return;

        _context.Allocate(newSize, out ByteString newBuffer);
        new Span<TNumber>(_buffer.Ptr, _count).CopyTo(new Span<TNumber>(newBuffer.Ptr, _count));
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
