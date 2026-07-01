using System;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

public static class IlEmitterShared
{
    public static readonly FieldInfo CtxBitmaps = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Bitmaps));
    public static readonly FieldInfo CtxLimit = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Limit));
    public static readonly FieldInfo CtxOpLimit = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.OpLimit));

    public static readonly MethodInfo GetTimestamp =
        typeof(Stopwatch).GetMethod(nameof(Stopwatch.GetTimestamp))!;
    public static readonly MethodInfo RecordMetrics =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordMetrics))!;
    public static readonly MethodInfo RunEntryScanMethod =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RunEntryScan))!;

    public static readonly MethodInfo AndWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo LazyOrWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.LazyOrWith),
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo AndNotWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndNotWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo Clear =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Clear), Type.EmptyTypes)!;
    public static readonly MethodInfo IsEmptyGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.IsEmpty))!.GetGetMethod()!;
    public static readonly MethodInfo ComputeCountMethod = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.ComputeCount), Type.EmptyTypes)!;
    public static readonly MethodInfo RepairAfterLazy =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.RepairAfterLazy), Type.EmptyTypes)!;

    public static readonly MethodInfo ThrowIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    public static readonly FieldInfo CtxResolvedMatches =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ResolvedMatches));
    public static readonly FieldInfo CtxInRangeCounts =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.InRangeCounts));
    public static readonly FieldInfo CtxCardinalities =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Cardinalities));
    public static readonly FieldInfo CtxEntryScanTakenAtOp =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.EntryScanTakenAtOp));
    public static readonly FieldInfo CtxForcedEntryScanGate =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ForcedEntryScanGate));
    public static readonly FieldInfo CtxToken =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Token));

    public static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes)!;

    public static readonly MethodInfo CtxFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromPostingSource))!;
    public static readonly MethodInfo CtxFillAllEntries = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillAllEntries))!;
    public static readonly MethodInfo CtxFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromTreeScan))!;
    public static readonly MethodInfo CtxFillFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromMatch))!;
    public static readonly MethodInfo CtxOrFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromPostingSource))!;
    public static readonly MethodInfo CtxOrFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromTreeScan))!;
    public static readonly MethodInfo CtxOrWithMatchSlot = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrWithMatchSlot))!;
    public static readonly MethodInfo CtxAndFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromPostingSource))!;
    public static readonly MethodInfo CtxAndFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromTreeScan))!;
    public static readonly MethodInfo CtxAndFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromMatch))!;
    public static readonly MethodInfo CtxAndNotFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromPostingSource))!;
    public static readonly MethodInfo CtxAndNotFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromTreeScan))!;
    public static readonly MethodInfo CtxAndNotFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromMatch))!;

    private static readonly Type ReadOnlySpanOfT0 = typeof(ReadOnlySpan<>).MakeGenericType(Type.MakeGenericMethodParameter(0));

    public static readonly MethodInfo SequenceCompareTo = typeof(MemoryExtensions)
        .GetMethod(nameof(MemoryExtensions.SequenceCompareTo), 1,
            BindingFlags.Public | BindingFlags.Static, null,
            [ReadOnlySpanOfT0, ReadOnlySpanOfT0], null)!
        .MakeGenericMethod(typeof(byte));

    public static readonly MethodInfo SequenceEqual = typeof(MemoryExtensions)
        .GetMethod(nameof(MemoryExtensions.SequenceEqual), 1,
            BindingFlags.Public | BindingFlags.Static, null,
            [ReadOnlySpanOfT0, ReadOnlySpanOfT0], null)!
        .MakeGenericMethod(typeof(byte));

    public static readonly MethodInfo ShouldSwitchToEntryScan =
        typeof(QueryPrimitives).GetMethod(
            nameof(QueryPrimitives.ShouldSwitchToEntryScan),
            [typeof(int), typeof(int), typeof(long), typeof(long)])!;

    public static readonly FieldInfo ResidualLongs =
        typeof(QueryExecution).GetField(nameof(QueryExecution.LongValues))!;
    public static readonly FieldInfo ResidualDoubles =
        typeof(QueryExecution).GetField(nameof(QueryExecution.DoubleValues))!;
    public static readonly FieldInfo AnalyzedSlices =
        typeof(QueryExecution).GetField(nameof(QueryExecution.AnalyzedSlices))!;
    public static readonly FieldInfo ResidualStringValues =
        typeof(QueryExecution).GetField(nameof(QueryExecution.StringValues))!;
    public static readonly FieldInfo ResidualFieldRootPages =
        typeof(QueryExecution).GetField(nameof(QueryExecution.FieldRootPages))!;
    public static readonly FieldInfo ResidualParamSlot1 =
        typeof(QueryExecution).GetField(nameof(QueryExecution.ResidualParamSlot1))!;
    public static readonly FieldInfo ResidualParamSlot2 =
        typeof(QueryExecution).GetField(nameof(QueryExecution.ResidualParamSlot2))!;

    public static readonly MethodInfo ReaderReset =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset));
    public static readonly MethodInfo ReaderFindNext =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext));
    public static readonly FieldInfo ReaderCurrentLong =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong));
    public static readonly FieldInfo ReaderCurrentDouble =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble));
    public static readonly FieldInfo ReaderCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current));
    public static readonly FieldInfo ReaderIsNull =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.IsNull));
    public static readonly FieldInfo ReaderIsNonExisting =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.IsNonExisting));

    public static readonly MethodInfo SpanLongLength =
        typeof(Span<long>).GetMethod("get_Length")!;
    public static readonly MethodInfo SpanIntLength =
        typeof(Span<int>).GetMethod("get_Length")!;
    public static readonly MethodInfo SpanLongGetItem =
        typeof(Span<long>).GetMethod("get_Item", [typeof(int)])!;
    public static readonly MethodInfo SpanIntGetItem =
        typeof(Span<int>).GetMethod("get_Item", [typeof(int)])!;
    public static readonly MethodInfo SpanEntryTermsReaderGetItem =
        typeof(Span<EntryTermsReader>).GetMethod("get_Item", [typeof(int)])!;

    public static readonly MethodInfo SliceAsReadOnlySpan =
        typeof(Slice).GetMethod(nameof(Slice.AsReadOnlySpan));

    public static readonly MethodInfo CheckFieldTermStartsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermStartsWith));
    public static readonly MethodInfo CheckFieldTermEndsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermEndsWith));

    public static readonly MethodInfo CheckFieldTermInSlice =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermInSlice));
    public static readonly MethodInfo CheckFieldTermInLong =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermInLong));
    public static readonly MethodInfo CheckFieldTermInDouble =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermInDouble));
    public static readonly MethodInfo CheckFieldTermAllInSlice =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermAllInSlice));
    public static readonly MethodInfo CheckFieldTermAllInLong =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermAllInLong));
    public static readonly MethodInfo CheckFieldTermAllInDouble =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermAllInDouble));

    public static readonly FieldInfo ResidualInSets =
        typeof(QueryExecution).GetField(nameof(QueryExecution.ResidualInSets))!;
    public static readonly FieldInfo ResidualInValuesBase =
        typeof(ResidualInValues).GetField(nameof(ResidualInValues.Base))!;
    public static readonly FieldInfo ResidualInValuesCount =
        typeof(ResidualInValues).GetField(nameof(ResidualInValues.Count))!;
    public static readonly FieldInfo ResidualInValuesHasNull =
        typeof(ResidualInValues).GetField(nameof(ResidualInValues.HasNull))!;

    // Constructs ReadOnlySpan<T>(T[] array, int start, int length) over the flat per-execution value
    // arrays, so the IN helpers receive the [Base, Base+Count) window without a per-predicate copy.
    public static readonly ConstructorInfo ReadOnlySpanLongCtor =
        typeof(ReadOnlySpan<long>).GetConstructor([typeof(long[]), typeof(int), typeof(int)])!;
    public static readonly ConstructorInfo ReadOnlySpanDoubleCtor =
        typeof(ReadOnlySpan<double>).GetConstructor([typeof(double[]), typeof(int), typeof(int)])!;
    public static readonly ConstructorInfo ReadOnlySpanSliceCtor =
        typeof(ReadOnlySpan<Slice>).GetConstructor([typeof(Slice[]), typeof(int), typeof(int)])!;

    public static void EmitLdcI4(ILGenerator il, int value)
    {
        switch (value)
        {
            case 0: il.Emit(OpCodes.Ldc_I4_0); break;
            case 1: il.Emit(OpCodes.Ldc_I4_1); break;
            case 2: il.Emit(OpCodes.Ldc_I4_2); break;
            case 3: il.Emit(OpCodes.Ldc_I4_3); break;
            case 4: il.Emit(OpCodes.Ldc_I4_4); break;
            case 5: il.Emit(OpCodes.Ldc_I4_5); break;
            case 6: il.Emit(OpCodes.Ldc_I4_6); break;
            case 7: il.Emit(OpCodes.Ldc_I4_7); break;
            case 8: il.Emit(OpCodes.Ldc_I4_8); break;
            default:
                if (value is >= -128 and <= 127)
                    il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                else
                    il.Emit(OpCodes.Ldc_I4, value);
                break;
        }
    }

}
