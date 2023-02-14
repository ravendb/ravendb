using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
namespace Corax.Utils;

public static class EntryIdEncodings
{
    //Schema:
    //[CONTAINER_ID][FREQUENCY][CONTAINER_TYPE]
    
    // Positions:
    private const int FrequencySizeInBits = 8;
    private const int EntryIdOffset = FrequencySizeInBits + ContainerTypeOffset;
    private const long Mask = 0xFFL;
    private const int ContainerTypeOffset = 2;
    
    // Quantization parameters:
    private const long Min = 0;
    private const long Max = 255;
    private const long QuantizationMax = 256 - 1;
    // ReSharper disable once PossibleLossOfFraction
    private const double QuantizationStep = (Max - Min) / (double)QuantizationMax;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long Encode(long entryId, short count, TermIdMask containerType)
    {
        Debug.Assert((count & ~Mask) == 0);
        return count << ContainerTypeOffset | entryId << EntryIdOffset | (long)containerType;
    }

    /// <summary>
    /// Returns id of entry without offset and frequency
    /// </summary>
    /// <param name="entryId"></param>
    /// <returns></returns>
    public static (long EntryId, short Frequency) Decode(long entryId)
    {
        return (entryId >> EntryIdOffset, (short)((entryId >> ContainerTypeOffset) & Mask));
    }

    /// <summary>
    /// Container id (page) without encodings.
    /// </summary>
    public static long GetContainerId(long entryId)
    {
        return (entryId >> EntryIdOffset);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static unsafe void Encode(Span<long> entries, ReadOnlySpan<short> frequencies, bool usePtrVersion = false)
    {
        Debug.Assert(frequencies.Length == entries.Length);

        if (usePtrVersion == false)
            goto Classic;
        
        fixed (long* entriesPtr = entries)
        fixed (short* frequenciesPtr = frequencies)
        {
            var currentEntryPtr = entriesPtr;
            var entriesPtrEnd = entriesPtr + entries.Length;
            var currentFrequency = frequenciesPtr;
            
            while (currentEntryPtr != entriesPtrEnd)
            {
                *currentEntryPtr = (*currentEntryPtr << EntryIdOffset) | FrequencyQuantization(*currentFrequency) << ContainerTypeOffset;

                currentEntryPtr++;
                currentFrequency++;
            }

            return;
        }
        
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
            frequencies[i] = (short)((matches[i] >> ContainerTypeOffset) & Mask);
            matches[i] >>= EntryIdOffset;
        }
    }
    
    private static long FrequencyQuantization(short freq)
    {
        if (freq > Max)
            return QuantizationMax; //MAX

        var output = Math.Min(QuantizationMax, (long)(freq / QuantizationStep));
        return output;
    }
}
