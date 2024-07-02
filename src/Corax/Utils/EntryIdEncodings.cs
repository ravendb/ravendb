using System;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Corax.Indexing;
using Sparrow;

namespace Corax.Utils;

public static class EntryIdEncodings
{
    //Schema:
    //[CONTAINER_ID | 54b][FREQUENCY | 8b][CONTAINER_TYPE | 2b]
    //There is a way to extend the container_id to 56 bits since the entryId is guaranteed to be divisible by 4, which means that the last two bits of it are empty.
    //Therefore, we could encode the two most significant bits of frequencies in that part.
    //However, using 54 bits gives us the possibility to index up to 18,014,398,509,481,984 entries.
    //At a speed ratio of 100,000 items per second, this would require approximately 5,712 years to complete.

    // Positions:
    private const byte FrequencySizeInBits = 8;
    private const byte EntryIdOffset = FrequencySizeInBits + ContainerTypeOffset;
    private const long Mask = 0xFFL;
    private const byte ContainerTypeOffset = 2;
    private const long MaxEntryId = 1L << 54;

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long Encode(long entryId, short count, TermIdMask containerType)
    {
        Debug.Assert(entryId < MaxEntryId);

        return FrequencyQuantization(count) << ContainerTypeOffset | entryId << EntryIdOffset | (long)containerType;
    }

    /// <summary>
    /// Returns id of entry without offset and frequency
    /// </summary>
    /// <param name="entryId"></param>
    /// <returns></returns>
    public static (long EntryId, short Frequency) Decode(long entryId)
    {
        return (entryId >> EntryIdOffset, FrequencyReconstructionFromQuantization(((entryId >> ContainerTypeOffset) & Mask)));
    }

    /// <summary>
    /// Container id (page) without encodings.
    /// </summary>
    public static long GetContainerId(long entryId)
    {
        return (entryId >> EntryIdOffset);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void Encode(Span<long> entries, Span<short> frequencies)
    {
        for (int idX = 0; idX < entries.Length; ++idX)
        {
            ref var entryId = ref Unsafe.Add(ref MemoryMarshal.GetReference(entries), idX); 
            ref var frequency = ref Unsafe.Add(ref MemoryMarshal.GetReference(frequencies), idX);
            entryId = (entryId << EntryIdOffset) | (FrequencyQuantization(frequency) << ContainerTypeOffset);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long DecodeAndDiscardFrequency(long entryId) => entryId >> EntryIdOffset;

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void DecodeAndDiscardFrequencyVector256(Span<long> entries, int read)
    {
        int idX = read - (read % Vector256<long>.Count);
        if (read < Vector256<long>.Count)
            goto Classic;

        ref var start = ref entries[0];
        int currentIdx = 0;
        while (currentIdx < idX)
        {
            ref var currentPtr = ref Unsafe.Add(ref start, currentIdx);
            var innerBuffer = Vector256.LoadUnsafe(ref currentPtr);
            var shiftRightLogical = Vector256.ShiftRightLogical(innerBuffer, EntryIdOffset);
            Vector256.StoreUnsafe(shiftRightLogical, ref currentPtr); 

            currentIdx += Vector256<long>.Count;
        }

        Classic:
        if (idX < read)
            DecodeAndDiscardFrequencyClassic(entries.Slice(idX), read - idX);
        
    }
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static void DecodeAndDiscardFrequencyNeon(Span<long> entries, int read)
    {
        Debug.Assert(AdvInstructionSet.Arm.IsSupported);

        int idX = read - (read % Vector128<long>.Count);
        if (read < Vector128<long>.Count)
            goto Classic;

        ref var start = ref entries[0];
        int currentIdx = 0;
        while (currentIdx < idX)
        {
            ref var currentPtr = ref Unsafe.Add(ref start, currentIdx);
            var innerBuffer = Vector128.LoadUnsafe(ref currentPtr);
            var shiftRightLogical = AdvSimd.ShiftRightLogical(innerBuffer, EntryIdOffset);
            Vector128.StoreUnsafe(shiftRightLogical, ref currentPtr); 
            
            currentIdx += Vector128<long>.Count;
        }

        Classic:
        if (idX < read)
            DecodeAndDiscardFrequencyClassic(entries.Slice(idX), read - idX);
        
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void DecodeAndDiscardFrequencyClassic(Span<long> entries, int read)
    {
        for (int i = read - 1; i >= 0; --i)
        {
            entries[i] >>= EntryIdOffset;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void DecodeAndDiscardFrequency(Span<long> entries, int read)
    {
        if (AdvInstructionSet.IsAcceleratedVector256)
            DecodeAndDiscardFrequencyVector256(entries, read);
        else if (AdvInstructionSet.Arm.IsSupported)
            DecodeAndDiscardFrequencyNeon(entries, read);
        else
            DecodeAndDiscardFrequencyClassic(entries, read);
    }


    public static void Decode(Span<long> matches, Span<short> frequencies)
    {
        for (int i = 0; i < matches.Length; ++i)
        {
            frequencies[i] = FrequencyReconstructionFromQuantization((short)((matches[i] >> ContainerTypeOffset) & Mask));
            matches[i] >>= EntryIdOffset;
        }
    }

    // 4 bit - exp (as 2^(4 + 2i) per step
    // 4 bit - mantissa
    private static readonly long[] Step = Enumerable.Range(0, 16).Select(i => (long)Math.Pow(2, 4 + 2 * i)).ToArray();

    private static readonly long[] StepSize = Enumerable.Range(0, 16).Select(i =>
        i == 0
            ? 16
            : (Step[i] - Step[i - 1])).ToArray();

    private static readonly double[] LevelSizeInStep = StepSize.Select(i => i / 16D).ToArray();

    private static readonly short[] FrequencyTable = Enumerable.Range(0, byte.MaxValue).Select(i => FrequencyReconstructionFromQuantizationFromFunction(i)).ToArray();

    internal static long QuantizeAndDequantize(short frequency)
    {
        return FrequencyReconstructionFromQuantization(FrequencyQuantization(frequency));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    internal static long FrequencyQuantization(short frequency)
    {
        if (frequency < 16)
            return frequency;

        var leadingZeros = BitOperations.LeadingZeroCount((uint)frequency);
        var level = (28 - leadingZeros + (leadingZeros & 0b1)) >> 1;
        var mod = (frequency - Step[level - 1]) / LevelSizeInStep[level];
        Debug.Assert((long)mod < 16);
        return ((long)(level << 4)) | (long)mod;
    }

    internal static long FrequencyQuantizationReference(short frequency)
    {
        if (frequency < 16) //shortcut
            return frequency;

        int level = 0;
        for (level = 0; level < 16 && frequency >= Step[level]; ++level)
        {
        } // look up for range

        var mod = (frequency - Step[level - 1]) / LevelSizeInStep[level];
        Debug.Assert((long)mod < 16);
        return ((long)(level << 4)) | (long)mod;
    }

    internal static short FrequencyReconstructionFromQuantization(long encoded) => FrequencyTable[encoded];


    internal static short FrequencyReconstructionFromQuantizationFromFunction(long encoded)
    {
        var level = (encoded & (0b1111 << 4)) >> 4;
        if (level == 0)
            return (short)encoded;

        var mantissa = encoded & 0b1111;
        return (short)(Step[level - 1] + LevelSizeInStep[level] * mantissa);
    }

    //Posting list contains encoded ids only but in matches we deal with decoded.
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    internal static long PrepareIdForPruneInPostingList(long entryId) => entryId << EntryIdEncodings.EntryIdOffset + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    internal static long PrepareIdForSeekInPostingList(long entryId) => entryId << EntryIdEncodings.EntryIdOffset;
}
