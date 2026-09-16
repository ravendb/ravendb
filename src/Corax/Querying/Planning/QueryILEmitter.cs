using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;

namespace Corax.Querying.Planning;

public static class QueryIlEmitter
{
    public delegate void CompiledExecuteDelegate(CompiledQueryMatch ctx);

    // Span<long>
    private static readonly ConstructorInfo SpanCtor = typeof(Span<long>).GetConstructor([typeof(void*), typeof(int)])!;

    public static CompiledExecuteDelegate EmitDelegate(PlanOp[] ops, out string csharpSource)
    {
        if (ops == null || ops.Length == 0)
        {
            csharpSource = "// Empty plan.\n";
            return EmptyExecute;
        }

        var dm = new DynamicMethod(
            "CompiledQuery",
            typeof(void),
            [typeof(CompiledQueryMatch)],
            typeof(CompiledQueryMatch).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();
        var cs = new StringBuilder();
        var d = new DualEmit(il, cs);

        d.CsLine("""
                 [SkipLocalsInit]
                 static void CompiledQuery(CompiledQueryMatch ctx)
                 {
                 """);
        
        // cursor - the leaf cursor we currently operate on: ctx.Leaves[cursor] / ctx.ResolvedMatches[cursor]
        // slot - the _destination_ bitmap for the operation: ctx.Bitmaps[slot]

        // Locals
        var bufferLocal = d.DeclareLocal(typeof(Span<long>), "buffer");
        var startTickLocal = d.DeclareLocal(typeof(long), "startTick");
        var cursorVar = d.DeclareLocal(typeof(int), "cursor");

        // Labels
        var doneLabel = d.DefineNamedLabel("Done");
        var entryScanLabel = d.DefineNamedLabel("EntryScan");
        bool hasEntryScan = false;
        bool needsLazyRepair = false;

        // stackalloc long[FillBufferSize]
        EmitStackAlloc(ref d, bufferLocal);

        // cursor = 0
        d.StoreLocalConst(cursorVar, 0);

        int lastEffectiveIndex = GetLastEffectiveIndex(ops);

        // OpLimit starts unlimited; arm it (= ctx.Limit) on the first slot-0 op after which we only add, never subtracts
        bool opLimitArmed = false;

        int lastNarrowingIndex = ComputeLastNarrowingIndex(ops);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            bool isLastEffectiveOp = i >= lastEffectiveIndex;
            bool emitGoToEmpty = !op.SkipEarlyExit && !isLastEffectiveOp;
            bool emitGotoLimitReached = op.BitmapLocal == 0 && !isLastEffectiveOp;

            // The cursor advance after the last cursor-consuming op is a dead store 
            bool advanceCursor = !isLastEffectiveOp;

            // True once no op after i narrows slot 0 — see lastNarrowingIndex above.
            bool noLaterNarrowing = i >= lastNarrowingIndex;

            // If we can only ever grow the set of results, we should check if we reached the limit early
            bool shouldCheckLimitReached = op.BitmapLocal == 0 && !isLastEffectiveOp && noLaterNarrowing;

            if (op.DebugLabel != null)
                d.SetPendingComment(op.DebugLabel);

            if (!opLimitArmed && op.BitmapLocal == 0 && noLaterNarrowing)
            {
                d.EmitArmOpLimit();
                opLimitArmed = true;
            }

            bool timeThisOp = IsTimedOp(op.Kind);
            if (timeThisOp)
                EmitTimingStart(ref d, startTickLocal, i);

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromPostingSource, op.BitmapLocal, advanceCursor);
                    if (shouldCheckLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.FillFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromTreeScan, op.BitmapLocal, advanceCursor);
                    if (shouldCheckLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.FillFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromMatch, op.BitmapLocal, advanceCursor);
                    if (shouldCheckLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.FillAllEntries:
                    d.EmitFillAllEntries(op.BitmapLocal);
                    break;

                case PlanOpKind.AndFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromPostingSource, op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel);
                    break;

                case PlanOpKind.AndFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromTreeScan, op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel);
                    break;

                case PlanOpKind.AndFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromMatch, op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel);
                    break;

                case PlanOpKind.OrFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromPostingSource, op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.OrFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromTreeScan, op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.OrFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrWithMatchSlot, op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.AndNotFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromPostingSource, op.BitmapLocal, advanceCursor);
                    break;

                case PlanOpKind.AndNotFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromTreeScan, op.BitmapLocal, advanceCursor);
                    break;

                case PlanOpKind.AndNotFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromMatch, op.BitmapLocal, advanceCursor);
                    break;

                case PlanOpKind.ClearBitmap:
                    d.EmitBitmapUnaryCall(op.BitmapLocal, IlEmitterShared.Clear);
                    break;

                case PlanOpKind.AndBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.AndWith);
                    break;

                case PlanOpKind.AndNotBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.AndNotWith);
                    break;

                case PlanOpKind.LazyOrBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.LazyOrWith);
                    needsLazyRepair = true;
                    break;

                case PlanOpKind.GotoDoneIfEmpty:
                    // Dead when terminal: `if (empty) goto Done;` falls straight through to the Done label, let's skip it then
                    if (isLastEffectiveOp is false)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel);
                    break;

                case PlanOpKind.MaybeEntryScan:
                    hasEntryScan = true;
                    EmitEntryScanCheck(ref d, cursorVar, entryScanLabel);
                    break;

                case PlanOpKind.InRangeFromPostingSource:
                    EmitInRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxOrFillFromPostingSource, i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.InRangeFromMatch:
                    EmitInRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxOrWithMatchSlot, i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel);
                    break;

                case PlanOpKind.AllInRangeFromPostingSource:
                    EmitInRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxAndFromPostingSource, i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.AllInRangeFromMatch:
                    EmitInRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxAndFromMatch, i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.GotoDone:
                    // Falls straight through to the Done label when terminal — emitting the branch
                    // would be dead IL. Only emit when something real follows.
                    if (!isLastEffectiveOp)
                        d.EmitGotoDone(doneLabel);
                    break;
            }

            if (timeThisOp)
                EmitTimingEnd(ref d, i, op.BitmapLocal, startTickLocal);
        }

        d.MarkLabel(doneLabel);
        if (needsLazyRepair)
            d.EmitBitmapUnaryCall(0, IlEmitterShared.RepairAfterLazy);
        d.EmitRetVoid();

        if (hasEntryScan)
            EmitEntryScanTail(ref d, entryScanLabel, cursorVar);
        else
        {
            // Dead label — must be marked even if unreachable (IL verifier requires it).
            d.Il.MarkLabel(entryScanLabel.Il);
            d.Il.Emit(OpCodes.Ret);
        }

        d.CsLine("}");

        csharpSource = cs.ToString();
        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    private static int ComputeLastNarrowingIndex(PlanOp[] ops)
    {
        ReadOnlySpan<PlanOpKind> narrowingOps =
        [
            PlanOpKind.AndFromPostingSource,
            PlanOpKind.AndFromTreeScan,
            PlanOpKind.AndFromMatch,
            PlanOpKind.AndNotFromPostingSource,
            PlanOpKind.AndNotFromTreeScan,
            PlanOpKind.AndNotFromMatch,
            PlanOpKind.AndBitmaps,
            PlanOpKind.AndNotBitmaps,
            PlanOpKind.AllInRangeFromPostingSource,
            PlanOpKind.AllInRangeFromMatch
        ];
        
        for (int i = ops.Length - 1; i >= 0; i--)
        {
            ref var op = ref ops[i];
            if (op.Kind is PlanOpKind.MaybeEntryScan)
                return i; // always counts as narrowing: it reads slot 0 as its candidate set and only removes

            if (op.BitmapLocal is not 0) // only ops writing the primary result bitmap can narrow it
                continue;

            if (narrowingOps.Contains(op.Kind)) // all those ops can _remove_ matches
                return i;
        }

        return -1;
    }

    private static int GetLastEffectiveIndex(PlanOp[] ops)
    {
        // We want to avoid emitting "goto Done; Done:", so we find the last _real_ op, skipping any trailing GotoDone / GotoDoneIfEmpty.
        for (int i = ops.Length - 1; i >= 0; i--)
        {
            if (ops[i].Kind is not (PlanOpKind.GotoDone or PlanOpKind.GotoDoneIfEmpty))
                return i;
        }
        return -1;
    }

    /// <summary>stackalloc long[FillBufferSize] → bufferLocal</summary>
    private static void EmitStackAlloc(ref DualEmit d, LocalBuilder bufferLocal)
    {
        IlEmitterShared.EmitLdcI4(d.Il, QueryPrimitives.FillBufferSize);
        d.Il.Emit(OpCodes.Conv_U);
        d.Il.Emit(OpCodes.Sizeof, typeof(long));
        d.Il.Emit(OpCodes.Mul_Ovf_Un);
        d.Il.Emit(OpCodes.Localloc);
        IlEmitterShared.EmitLdcI4(d.Il, QueryPrimitives.FillBufferSize);
        d.Il.Emit(OpCodes.Newobj, SpanCtor);
        d.Il.Emit(OpCodes.Stloc, bufferLocal);
        d.CsLine($"Span<long> {d.GetLocalName(bufferLocal)} = stackalloc long[{QueryPrimitives.FillBufferSize}];");
    }

    private static bool IsTimedOp(PlanOpKind kind) => // these require no timing
        kind is not (PlanOpKind.GotoDoneIfEmpty or PlanOpKind.MaybeEntryScan or PlanOpKind.GotoDone or PlanOpKind.ClearBitmap);

    /// <summary>startTick = Stopwatch.GetTimestamp()</summary>
    private static void EmitTimingStart(ref DualEmit d, LocalBuilder startTickLocal, int opIndex)
    {
        d.Il.Emit(OpCodes.Call, IlEmitterShared.GetTimestamp);
        d.Il.Emit(OpCodes.Stloc, startTickLocal);
        d.CsLine($"long startTick_{opIndex} = Stopwatch.GetTimestamp();");
    }

    // record the timing & result count per operation, only actually does something when `include timings()` is set
    private static void EmitTimingEnd(ref DualEmit d, int opIndex, int slot, LocalBuilder startTickLocal)
    {
        d.Il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(d.Il, opIndex);
        d.Il.Emit(OpCodes.Ldloc, startTickLocal);
        IlEmitterShared.EmitLdcI4(d.Il, slot);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RecordMetrics);

        d.CsLine($"CompiledQueryHelper.RecordMetrics(ctx, {opIndex}, startTick_{opIndex}, {slot});");
        d.CsLine("");
    }

    /// <summary>if (ShouldSwitchToEntryScan(ctx.ForcedEntryScanGate, cursor, bitmaps[0].ComputeCount(), ctx.Cardinalities[cursor])) goto EntryScan.
    /// The cursor doubles as this gate's index, so the $rvn_corax_entry_scan override can target it.</summary>
    private static void EmitEntryScanCheck(ref DualEmit d, LocalBuilder cursorVar, LabelPair entryScanLabel)
    {
        // ctx.ForcedEntryScanGate, cursor
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxForcedEntryScanGate);
        d.Il.Emit(OpCodes.Ldloc, cursorVar);

        d.IlLoadBitmapRef(0);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.ComputeCountMethod);
        d.Il.Emit(OpCodes.Conv_I8);

        // ctx.Cardinalities[cursor]
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxCardinalities);
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Ldelem_I8);

        d.Il.Emit(OpCodes.Call, IlEmitterShared.ShouldSwitchToEntryScan);
        d.Il.Emit(OpCodes.Brtrue, entryScanLabel.Il);

        d.CsLine($"if (QueryPrimitives.ShouldSwitchToEntryScan(ctx.ForcedEntryScanGate, {d.GetLocalName(cursorVar)}, ctx.Bitmaps[0].ComputeCount(), ctx.Cardinalities[{d.GetLocalName(cursorVar)}]))");
        d.CsLine($"    goto {entryScanLabel.Name};");
    }

    /// <summary>Emit the OR/AND range loop over IN-expanded term slots.</summary>
    private static void EmitInRangeLoop(ref DualEmit d, LocalBuilder cursorVar, int rangeIdx, int bitmapLocal,
        MethodInfo method, int opIndex,
        bool earlyExit, bool skipEarlyExit, LabelPair doneLabel)
    {
        var loopVar = d.DeclareLocal(typeof(int), $"j_{opIndex}");
        var endVar = d.DeclareLocal(typeof(int), $"end_{opIndex}");
        var loopCheck = d.DefineLabelPair($"rangeCheck_{opIndex}");
        var loopBody = d.DefineLabelPair($"rangeBody_{opIndex}");

        // endVar = cursor + ctx.InRangeCounts[rangeIdx] (dual: drives the for-loop bound below)
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxInRangeCounts);
        IlEmitterShared.EmitLdcI4(d.Il, rangeIdx);
        d.Il.Emit(OpCodes.Ldelem_I4);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, endVar);
        d.CsLine($"{d.GetLocalName(endVar)} = {d.GetLocalName(cursorVar)} + ctx.InRangeCounts[{rangeIdx}];");

        // IL loop init: loopVar = cursor; goto check.   (IL only — the for-header carries this in C#)
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Stloc, loopVar);
        d.Il.Emit(OpCodes.Br, loopCheck.Il);

        // C# for-header + open brace (C# only).
        d.CsLine($"for ({d.GetLocalName(loopVar)} = {d.GetLocalName(cursorVar)}; {d.GetLocalName(loopVar)} < {d.GetLocalName(endVar)}; {d.GetLocalName(loopVar)}++)");
        d.CsLine("{");

        // Loop body (dual statements land inside the braces).
        d.Il.MarkLabel(loopBody.Il);
        d.IlCancellationCheck();

        // Both AND and OR primitives take the destination slot explicitly: ctx.Method(ctx, loopVar, bitmapLocal).
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldc_I4, bitmapLocal);
        d.Il.Emit(OpCodes.Call, method);
        d.CsCall($"QueryPrimitives.{method.Name}(ctx, {d.GetLocalName(loopVar)}, {bitmapLocal});");

        // AND short-circuits once the destination is empty (the intersection can only shrink).
        if (earlyExit && !skipEarlyExit)
        {
            d.EmitBitmapEmptyGoto(bitmapLocal, doneLabel);
        }

        // IL loopVar++ (IL only — the for-header carries this in C#).
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldc_I4_1);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, loopVar);

        // C# close brace (C# only).
        d.CsLine("}");

        // IL loop check: if (loopVar < endVar) goto loopBody.   (IL only)
        d.Il.MarkLabel(loopCheck.Il);
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldloc, endVar);
        d.Il.Emit(OpCodes.Blt, loopBody.Il);

        // cursor = endVar   (dual)
        d.Il.Emit(OpCodes.Ldloc, endVar);
        d.Il.Emit(OpCodes.Stloc, cursorVar);
        d.CsLine($"{d.GetLocalName(cursorVar)} = {d.GetLocalName(endVar)};");
    }

    /// <summary>EntryScan tail: set ctx.EntryScanTakenAtOp, run entry scan, return. RunEntryScan reads & writes candidates from slot 0 and uses slot 1 as scratch storage.</summary>
    private static void EmitEntryScanTail(ref DualEmit d, LabelPair entryScanLabel, LocalBuilder cursorVar)
    {
        d.MarkLabel(entryScanLabel);

        // ctx.EntryScanTakenAtOp = cursor
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Stfld, IlEmitterShared.CtxEntryScanTakenAtOp);
        d.CsLine($"ctx.EntryScanTakenAtOp = {d.GetLocalName(cursorVar)};");

        // CompiledQueryHelper.RunEntryScan(ctx, ref bitmaps[0], ref bitmaps[1])
        d.Il.Emit(OpCodes.Ldarg_0);
        d.IlLoadBitmapRef(0);
        d.IlLoadBitmapRef(1);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RunEntryScanMethod);
        d.CsLine("CompiledQueryHelper.RunEntryScan(ctx, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");

        d.EmitRetVoid();
    }

    private static void EmptyExecute(CompiledQueryMatch ctx) { }
}
