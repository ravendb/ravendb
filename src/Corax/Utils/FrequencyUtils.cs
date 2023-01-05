using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Corax.Utils;

internal static class FrequencyUtils
{
    private const int EntryIdOffset = 8 + ContainerTypeOffset;
    private const long Mask = 0xFFL;
    private const int ContainerTypeOffset = 2;

    private const long Min = 0;
    private const long Max = 512;
    private const long QuantizationMax = 256 - 1;
    
    // ReSharper disable once PossibleLossOfFraction
    private const double QuantizationStep = (Max - Min) / (double)QuantizationMax;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long Encode(long entryId, long count)
    {
        Debug.Assert((count & ~Mask) == 0 );
        return count << ContainerTypeOffset | entryId << EntryIdOffset;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long Encode(long entryId, long count, TermIdMask containerType)
    {
        Debug.Assert((count & ~Mask) == 0 );
        return count << ContainerTypeOffset | entryId << EntryIdOffset | (long)containerType;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static (long EntryId, long Frequency) Decode(long entryId)
    {
        return (entryId >> EntryIdOffset, (entryId >> ContainerTypeOffset) & Mask);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void EncodeFrequencies(Span<long> entries, ReadOnlySpan<long> frequencies)
    {
        Debug.Assert(frequencies.Length == entries.Length);

        if (Avx2.IsSupported)
        {
            //todo: implement this
        }

        for (int i = 0; i < entries.Length; ++i)
        {
            Debug.Assert(entries[i] >= 0);
            
            //Debug.Assert((FreqEncoder(frequencies[i]) & ~Mask) == 0 );


            var ecd = FreqEncoder(frequencies[i]) << ContainerTypeOffset | entries[i] << EntryIdOffset;
            if (ecd <= 0) Debugger.Break();
            entries[i] = ecd;
        }


        long FreqEncoder(long freq)
        {
            return freq;
            if (freq > Max)
                return QuantizationMax; //MAX

            var output = Math.Min(QuantizationMax, (long)(freq/QuantizationStep));
            if (output <= 0)
                Debugger.Break();
            return output;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static long RemoveFrequency(long entryId) => entryId >> EntryIdOffset;

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public static void RemoveFrequencies(Span<long> entries)
    {
        for (int i = 0; i < entries.Length; ++i)
        {
            entries[i] >>= EntryIdOffset;
        }
    }

    public static void DecodeBulk(Span<long> matches, Span<long> copy, Span<float> scores)
    {
        for (int i = 0; i < matches.Length; ++i)
        {
            scores[i] = (matches[i] >> ContainerTypeOffset) & Mask;
            matches[i] >>= EntryIdOffset;
            copy[i] = matches[i];
        }
    }
}
