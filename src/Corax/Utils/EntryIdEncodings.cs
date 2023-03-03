using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Server.Binary;

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
    
    //todo perf
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe void Encode(Span<long> entries, Span<short> frequencies, bool usePtrVersion = false)
    {
        Debug.Assert(frequencies.Length == entries.Length);

        if (usePtrVersion == false)
            goto Classic;

        for (int idX = 0; idX < entries.Length; ++idX)
        {
            ref var entryId = ref Unsafe.Add(ref MemoryMarshal.GetReference(entries), idX);
            ref var frequency = ref Unsafe.Add(ref MemoryMarshal.GetReference(frequencies), idX);
            entryId = entryId << EntryIdOffset | FrequencyQuantization(frequency) << ContainerTypeOffset;
        }

        return;

        Classic:
        for (int i = 0; i < entries.Length; ++i)
        {
            var ecd = FrequencyQuantization(frequencies[i]) << ContainerTypeOffset | entries[i] << EntryIdOffset;
            entries[i] = ecd;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long DecodeAndDiscardFrequency(long entryId) => entryId >> EntryIdOffset;

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void DecodeAndDiscardFrequency(Span<long> entries, int read)
    {
        for (int i = 0; i < read; ++i)
        {
            entries[i] >>= EntryIdOffset;
        }
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
    
    //todo perf
    internal static long FrequencyQuantization(short frequency)
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
    internal static long PrepareIdForPruneInPostingList(long entryId) => entryId << EntryIdEncodings.EntryIdOffset + 1;
    
    internal static long PrepareIdForSeekInPostingList(long entryId) => entryId << EntryIdEncodings.EntryIdOffset;

}
