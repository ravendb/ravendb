using System;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Utils;

public interface IBufferGrowth
{
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
        var size = currentSizeInBytes > 16 * Sparrow.Global.Constants.Size.Megabyte
            ? (int)(currentSizeInBytes * 1.5)
            : currentSizeInBytes * 2;

        // Represent array as N*sizeof(long)
        return size - (size % Unsafe.SizeOf<TNumber>());
    }

    public bool GrowingThresholdExceed(in int count, in int sizeInBytes)
    {
        // 1/16 left
        var amountOfLongs = (sizeInBytes / Unsafe.SizeOf<TNumber>());
        return (amountOfLongs - count) < amountOfLongs / 16;
    }

    public int GetInitialSize(in long initialSize)
    {
        var size = 4 * Math.Min(Math.Max(Sparrow.Global.Constants.Size.Kilobyte, (int)initialSize), 16 * Sparrow.Global.Constants.Size.Kilobyte);
        // Represent array as N*sizeof(long)
        return size - (size % Unsafe.SizeOf<TNumber>());
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
    public int Count => _count;
    public bool IsInitialized;
    
    public int Capacity => IsInitialized ? _buffer.Length / sizeof(TNumber) : 0;

    public Span<TNumber> GetSpace()
    {
        if (_growthCalculator.GrowingThresholdExceed(_count, _buffer.Length))
            Grow();

        return _buffer.ToSpan<TNumber>().Slice(_count);
    }

    public Span<TNumber> Results => _buffer.ToSpan<TNumber>().Slice(0, _count);
    
    public bool HasEmptySpace => _buffer.Length == (_count * sizeof(TNumber));

    public GrowableBuffer()
    {
    }

    public void AddUsage(in int count) => _count += count;

    public void Truncate(in int newCount) => _count = newCount;
    
    public void Init(ByteStringContext context, in long initialSize)
    {
        _context = context;
        _context.Allocate(_growthCalculator.GetInitialSize(initialSize * sizeof(TNumber)), out _buffer);
        IsInitialized = true;
    }

    private void Grow()
    {
        var newSize = _growthCalculator.GetNewSize(_buffer.Length);
        _context.Allocate(newSize, out ByteString newBuffer);
        new Span<TNumber>(_buffer.Ptr, _count).CopyTo(new Span<TNumber>(newBuffer.Ptr, _count));
        _context.Release(ref _buffer);
        _buffer = newBuffer;
    }
    
    public void Dispose()
    {
        _context.Release(ref _buffer);
        _buffer = default;
    }
}
