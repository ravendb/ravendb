using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Voron.Data.RoaringBitmaps;

public unsafe struct ContainerEntry
{
    /// <summary>
    /// Direct pointer to container data for Array, ArrayUnsorted, and Bitmap containers; for Range
    /// containers it stores RangeStart encoded as (RangeStart + 1) to avoid an allocation.
    /// </summary>
    public byte* Data;

    /// <summary>Memory handle for disposal. Default for Range containers.</summary>
    internal ByteString Storage;

    public int Cardinality;

    /// <summary>
    /// Container key (value >> 16); lets us walk entries without index indirection.
    /// Supports up to ~140T entries (2^47).
    /// </summary>
    public uint Key;

    internal uint NextFreeSlot
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Key;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => Key = value;
    }


    public ushort* ArrayData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort*)Data;
    }

    /// <summary>Raw ulong pointer for SIMD operations and methods requiring pointers.</summary>
    public ulong* BitmapPtr
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ulong*)Data;
    }

    /// <summary>
    /// Start offset (0..65535) for Range containers. Encoded in Data as (start + 1),
    /// so start=0 remains representable.
    /// </summary>
    internal ushort RangeStart
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort)((nuint)Data - 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => Data = (byte*)(nuint)(value + 1);
    }
}
