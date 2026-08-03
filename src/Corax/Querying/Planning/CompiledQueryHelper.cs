using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Voron;
using Voron.Data.Containers;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

public static class CompiledQueryHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordMetrics(CompiledQueryMatch ctx, int opIndex, long startTick, int slot)
    {
        if(ctx.Timings is null) return;
        
        ctx.Timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
        ctx.ResultCounts[opIndex] = ctx.Bitmaps[slot].ComputeCount();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermStartsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> prefix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull || reader.IsNonExisting)
                continue; // Current holds a stale key for null/non-existing terms — don't match against it
            if (reader.Current.Decoded().StartsWith(prefix))
                return true;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermEndsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> suffix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull || reader.IsNonExisting)
                continue; // Current holds a stale key for null/non-existing terms — don't match against it
            if (reader.Current.Decoded().EndsWith(suffix))
                return true;
        }

        return false;
    }

    private interface ITermComparer<in T>
    {
        bool Matches(ref EntryTermsReader reader, T value);
    }

    private struct SliceComparer : ITermComparer<Slice>
    {
        public bool Matches(ref EntryTermsReader reader, Slice value) => reader.Current.Decoded().SequenceEqual(value.AsReadOnlySpan());
    }

    private struct LongComparer : ITermComparer<long>
    {
        public bool Matches(ref EntryTermsReader reader, long value) => reader.CurrentLong == value;
    }

    private struct DoubleComparer : ITermComparer<double>
    {
        // ReSharper disable once CompareOfFloatsByEqualityOperator
        public bool Matches(ref EntryTermsReader reader, double value) => reader.CurrentDouble == value;
    }

    public static bool CheckFieldTermAllInSlice(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<Slice> values, bool includeNull)
        => CheckFieldTermAllIn<Slice, SliceComparer>(ref reader, fieldRootPage, values, includeNull);

    public static bool CheckFieldTermAllInLong(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<long> values, bool includeNull)
        => CheckFieldTermAllIn<long, LongComparer>(ref reader, fieldRootPage, values, includeNull);

    public static bool CheckFieldTermAllInDouble(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<double> values, bool includeNull)
        => CheckFieldTermAllIn<double, DoubleComparer>(ref reader, fieldRootPage, values, includeNull);

    
    public static bool CheckFieldTermInSlice(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<Slice> values, bool includeNull)
        => CheckFieldTermIn<Slice, SliceComparer>(ref reader, fieldRootPage, values, includeNull);

    public static bool CheckFieldTermInLong(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<long> values, bool includeNull)
        => CheckFieldTermIn<long, LongComparer>(ref reader, fieldRootPage, values, includeNull);

    public static bool CheckFieldTermInDouble(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<double> values, bool includeNull)
        => CheckFieldTermIn<double, DoubleComparer>(ref reader, fieldRootPage, values, includeNull);

    private static bool CheckFieldTermIn<T, TComparer>(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<T> values, bool includeNull)
        where TComparer : struct, ITermComparer<T>
    {
        TComparer compare = default;
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNonExisting)
                continue;
            if (reader.IsNull)
            {
                if (includeNull)
                    return true;
                continue;
            }

            for (int k = 0; k < values.Length; k++)
            {
                if (compare.Matches(ref reader, values[k]))
                    return true;
            }
        }

        return false;
    }

    private static bool CheckFieldTermAllIn<T, TComparer>(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<T> values, bool includeNull)
        where TComparer : struct, ITermComparer<T>
    {
        // we need to handle ALL IN ($terms), with null as an option, as well ass $terms = [x,x] - so the same value repeated
        
        TComparer compare = default;
        bool hasNull = false;
        for (int chunk = 0; chunk < values.Length; chunk += 64)
        {
            ReadOnlySpan<T> window = values.Slice(chunk, Math.Min(64, values.Length - chunk));
            ulong allMatched = window.Length == 64 ? ulong.MaxValue : (1UL << window.Length) - 1;
            ulong matched = 0;

            reader.Reset();
            while (matched != allMatched && reader.FindNext(fieldRootPage))
            {
                if (reader.IsNonExisting)
                    continue;

                if (reader.IsNull)
                {
                    hasNull = true;
                    continue;
                }

                for (int i = 0; i < window.Length; i++)
                {
                    ulong bit = 1UL << i;
                    if ((matched & bit) == 0 && compare.Matches(ref reader, window[i]))
                    {
                        matched |= bit;
                        // intentionally not breaking here, we may have ['x','x'] in the terms to search, and we want to get all of them 
                        // break;
                    }
                }
            }

            if (matched != allMatched)
                return false;
        }

        return includeNull == false || hasNull;
    }

    /// <summary>Run entry scan: the IL-emitted predicate processes in batches, this method ensures that it gets them in an efficient manner.</summary>
    public static unsafe void RunEntryScan(CompiledQueryMatch ctx, ref RoaringBitmap sourceBitmap, ref RoaringBitmap targetBitmap)
    {
        long startTick = Stopwatch.GetTimestamp();

        Span<long> buffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<long> containerLocs = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<UnmanagedSpan> spans = stackalloc UnmanagedSpan[QueryPrimitives.EntryScanBatchSize];
        var readers = ArrayPool<EntryTermsReader>.Shared.Rent(QueryPrimitives.EntryScanBatchSize);

        var searcher = ctx.Searcher;
        var predicate = ctx.CompiledEntryPredicate;
        var llt = searcher.Transaction.LowLevelTransaction;
      
        targetBitmap.Clear();
        sourceBitmap.PrepareForReading();

        using var iterator = sourceBitmap.GetIterator();

        // The emitted predicate evaluates readers strictly one at a time, every reader in the batch can share a single key
        var entryKey = llt.AcquireCompactKey();
        try
        {
            // we deferred building the scan params (not always needed), not we need them
            var exec = ctx.Exec; 
            if (exec.PopulateScanParams is { } populate)
            {
                populate();
                exec.PopulateScanParams = null;
            }

            int read;
            while ((read = iterator.Fill(ref sourceBitmap, buffer)) > 0)
            {
                ctx.Token.ThrowIfCancellationRequested();

                var batch = buffer[..read];
                ctx.EntryScanEntriesScanned += read;

                Span<long> locs = containerLocs[..read];
                searcher.ResolveEntryLocations(batch, locs);
                locs.Sort(batch);
                Container.GetAll(llt, locs, spans, llt.PageLocator);
                searcher.InitializeSpecialTermsMarkers();

                int validCount = 0;
                for (int i = 0; i < read; i++)
                {
                    if (locs[i] == -1 || spans[i].Address == null)
                        continue;
                    readers[validCount] = new EntryTermsReader(llt,
                        searcher.NullTermsMarkers, searcher.NonExistingTermsMarkers,
                        spans[i].Address, spans[i].Length, searcher.DictionaryId,
                        searcher.VectorFieldsMarkers, entryKey);
                    batch[validCount] = batch[i]; // compact entry IDs in-place
                    validCount++;
                }

                if (validCount == 0)
                    continue;

                int passed = predicate(exec, readers.AsSpan(0, validCount), buffer[..validCount], Span<int>.Empty);
                ctx.EntryScanEntriesPassed += passed;
                var passedBuffer = batch[..passed];
                passedBuffer.Sort();
                targetBitmap.AddRange(passedBuffer);

                if (ctx.EntryScanEntriesPassed >= ctx.Limit)
                    break;
            }

            // Survivors were built into targetBitmap (slot 1). Swap the two bitmap structs so the output is at slot 0
            (sourceBitmap, targetBitmap) = (targetBitmap, sourceBitmap);
        }
        finally
        {
            llt.ReleaseCompactKey(ref entryKey);
            // clearArray: EntryTermsReader is a struct holding references (LowLevelTransaction, marker HashSets);
            ArrayPool<EntryTermsReader>.Shared.Return(readers, clearArray: true);
            ctx.EntryScanTiming = Stopwatch.GetTimestamp() - startTick;
        }
    }
}
