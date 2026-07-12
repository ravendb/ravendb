using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Text;
using Corax.Utils;

namespace Corax.Querying.Planning;

/// <summary>
/// Emits a delegate for entry-scan path (residuals after the query already did some work)
/// </summary>
public static class ResidualScanIlEmitter
{
    public delegate int ResidualScanPredicate(
        QueryExecution exec,
        Span<EntryTermsReader> readers,
        Span<long> entryIds,
        Span<int> originalIndexes);

    /// <summary>Emit a residual-scan delegate that evaluates <paramref name="predicates"/> against each <see cref="EntryTermsReader"/> in the batch.
    /// Surviving entry IDs (and their original indexes) are compacted to the front of <c>entryIds</c>/<c>originalIndexes</c>; the return value is the count of survivors.
    /// <paramref name="csharpSource"/> receives an equivalent C# source rendering of the emitted IL for debugging.</summary>
    public static ResidualScanPredicate EmitDelegate(Span<ScanPredicateInfo> predicates, out string csharpSource)
    {
        if (predicates.IsEmpty)
        {
            csharpSource = string.Empty;
            return null;
        }

        var dm = new DynamicMethod(
            "ResidualScan",
            typeof(int),
            [typeof(QueryExecution), typeof(Span<EntryTermsReader>), typeof(Span<long>), typeof(Span<int>)],
            typeof(QueryExecution).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();
        var cs = new StringBuilder();
        var d = new DualEmit(il, cs);

        var execIdx = d.RegisterArg("exec");
        var readersIdx = d.RegisterArg("readers");
        var entryIdsIdx =  d.RegisterArg("entryIds");
        var originalIndexesIdx = d.RegisterArg("originalIndexes");

        d.SetContextArg(execIdx);

        d.CsLine("static int ResidualScan(QueryExecution exec, Span<EntryTermsReader> readers, Span<long> entryIds, Span<int> originalIndexes)");
        d.CsLine("{");

        var iLocal = d.DeclareLocal(typeof(int), "i");
        var writeIdxLocal = d.DeclareLocal(typeof(int), "writeIdx");
        var lengthLocal = d.DeclareLocal(typeof(int), "length");
        var readerRefLocal = d.DeclareLocalRef(typeof(EntryTermsReader), "reader");

        // length = entryIds.Length
        EmitSpanLengthToLocal(ref d, entryIdsIdx, IlEmitterShared.SpanLongLength, lengthLocal);
        // writeIdx = 0
        d.StoreLocalConst(writeIdxLocal, 0);

        var loopCheck = d.DefineLabelPair("loopCheck");
        var loopBody = d.DefineLabelPair("loopBody");
        var loopIncrement = d.DefineLabelPair("loopInc");
        var rejected = d.DefineNamedLabel("rejected");
        bool needsRejectLabel = AnyNotEqual(predicates);

        // IL: i = 0; goto loopCheck; loopBody:
        d.Il.Emit(OpCodes.Ldc_I4_0);
        d.Il.Emit(OpCodes.Stloc, iLocal);
        d.Il.Emit(OpCodes.Br, loopCheck.Il);
        d.Il.MarkLabel(loopBody.Il);
        // C#: for (i = 0; i < length; i++) {
        d.CsLine($"for ({d.GetLocalName(iLocal)} = 0; {d.GetLocalName(iLocal)} < {d.GetLocalName(lengthLocal)}; {d.GetLocalName(iLocal)}++)");
        d.CsLine("{");

        // ref reader = ref readers[i]
        EmitSpanGetItemRef(ref d, readersIdx, IlEmitterShared.SpanEntryTermsReaderGetItem, iLocal, readerRefLocal);

        int rootIdx = 0;
        int inSetIdx = 0;
        int paramSlotIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            d.CsLine("");
            EmitPredicate(ref d, in predicates[p], rejected.Il, ref rootIdx, ref inSetIdx, ref paramSlotIdx, readerRefLocal, p);
        }

        // All passed: entryIds[writeIdx] = entryIds[i]
        EmitSpanElementCopy(ref d, entryIdsIdx, IlEmitterShared.SpanLongGetItem, writeIdxLocal, iLocal, OpCodes.Ldind_I8, OpCodes.Stind_I8);

        var noOrigIdx = d.DefineLabelPair("noOrigIdx");
        d.Il.Emit(OpCodes.Ldarga_S, originalIndexesIdx);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.SpanIntLength);
        d.Il.Emit(OpCodes.Ldc_I4_0);
        d.Il.Emit(OpCodes.Ceq);
        d.Il.Emit(OpCodes.Brtrue, noOrigIdx.Il);
        d.CsLine($"if ({d.GetArgName(originalIndexesIdx)}.Length != 0)");
        EmitSpanElementCopy(ref d, originalIndexesIdx, IlEmitterShared.SpanIntGetItem, writeIdxLocal, iLocal, OpCodes.Ldind_I4, OpCodes.Stind_I4);
        d.Il.MarkLabel(noOrigIdx.Il);

        // writeIdx++
        d.IncrementLocal(writeIdxLocal);

        d.Il.Emit(OpCodes.Br, loopIncrement.Il);

        // rejected: (IL always; the C# label only when a `!=` leaf rejects from inside its term-scan while)
        d.Il.MarkLabel(rejected.Il);
        if (needsRejectLabel)
            d.CsLine($"{RejectedLabel}:;");

        // C#: close the for body. IL: loopInc: i++; loopCheck: if (i < length) goto loopBody
        d.CsLine("}");
        d.Il.MarkLabel(loopIncrement.Il);
        d.Il.Emit(OpCodes.Ldloc, iLocal);
        d.Il.Emit(OpCodes.Ldc_I4_1);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, iLocal);
        d.Il.MarkLabel(loopCheck.Il);
        d.Il.Emit(OpCodes.Ldloc, iLocal);
        d.Il.Emit(OpCodes.Ldloc, lengthLocal);
        d.Il.Emit(OpCodes.Blt, loopBody.Il);

        // return writeIdx
        d.LoadLocal(writeIdxLocal);
        d.EmitReturn();

        d.CsLine("}");
        csharpSource = cs.ToString();

        return (ResidualScanPredicate)dm.CreateDelegate(typeof(ResidualScanPredicate));
    }

    /// <summary>target = arg.Length</summary>
    private static void EmitSpanLengthToLocal(ref DualEmit d, byte argIdx, MethodInfo lengthGetter, LocalBuilder target)
    {
        d.LoadArgAddress(argIdx);
        d.Il.Emit(OpCodes.Call, lengthGetter);
        d.Il.Emit(OpCodes.Stloc, target);
        var argName = d.CsStack.Pop();
        d.CsLine($"{d.GetLocalName(target)} = {argName}.Length;");
    }

    /// <summary>ref dest = ref arg[index]</summary>
    private static void EmitSpanGetItemRef(ref DualEmit d, byte argIdx, MethodInfo getItem,
        LocalBuilder indexLocal, LocalBuilder destRef)
    {
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, indexLocal);
        d.Il.Emit(OpCodes.Call, getItem);
        d.Il.Emit(OpCodes.Stloc, destRef);
        d.CsLine($"ref var {d.GetLocalName(destRef)} = ref {d.GetArgName(argIdx)}[{d.GetLocalName(indexLocal)}];");
    }

    /// <summary>arg[destIdx] = arg[srcIdx]</summary>
    private static void EmitSpanElementCopy(ref DualEmit d, byte argIdx, MethodInfo getItem,
        LocalBuilder destIdx, LocalBuilder srcIdx, OpCode loadIndirect, OpCode storeIndirect)
    {
        // &arg[destIdx]
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, destIdx);
        d.Il.Emit(OpCodes.Call, getItem);
        // arg[srcIdx] value
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, srcIdx);
        d.Il.Emit(OpCodes.Call, getItem);
        d.Il.Emit(loadIndirect);
        // store
        d.Il.Emit(storeIndirect);

        string argName = d.GetArgName(argIdx);
        d.CsLine($"{argName}[{d.GetLocalName(destIdx)}] = {argName}[{d.GetLocalName(srcIdx)}];");
    }

    /// <summary>If the predicate failed, we jump to the failLabel, success means falling from the end </summary>
    private static void EmitPredicate(ref DualEmit d, in ScanPredicateInfo pred, Label failLabel, ref int rootIdx, ref int inSetIdx, ref int paramSlotIdx, LocalBuilder readerRefLocal, int pIdx)
    {
        if (pred.SubPredicates == null)
        {
            EmitLeafPredicate(ref d, in pred, failLabel, ContinueTarget, rootIdx, ref inSetIdx, paramSlotIdx, readerRefLocal);
            if (ConsumesFieldRootPage(in pred))
                rootIdx++;
            if (ConsumesScalarParam(in pred))
                paramSlotIdx++;
            return;
        }

        if (pred.Group == GroupKind.Or)
        {
            var groupPassed = d.DefineLabelPair($"gp_{pIdx}");
            foreach (var predicate in pred.SubPredicates)
            {
                var nextSub = d.DefineLabelPair("nextBranch");
                EmitLeafPredicate(ref d, in predicate, nextSub.Il, nextSub.Name, rootIdx, ref inSetIdx, paramSlotIdx, readerRefLocal);
                // Branch succeeded — skip remaining alternatives.
                d.GotoAlways(groupPassed);
                d.MarkLabel(nextSub);
                if (ConsumesFieldRootPage(in predicate))
                    rootIdx++;
                if (ConsumesScalarParam(in predicate))
                    paramSlotIdx++;
            }

            // All branches fell through → group fails.
            d.Il.Emit(OpCodes.Br, failLabel);
            d.CsLine(Jump(ContinueTarget));
            d.MarkLabel(groupPassed);
        }
        else
        {
            foreach (var predicate in pred.SubPredicates)
            {
                EmitLeafPredicate(ref d, in predicate, failLabel, ContinueTarget, rootIdx, ref inSetIdx, paramSlotIdx, readerRefLocal);
                if (ConsumesFieldRootPage(in predicate))
                    rootIdx++;
                if (ConsumesScalarParam(in predicate))
                    paramSlotIdx++;
            }
        }
    }

    private static bool ConsumesFieldRootPage(in ScanPredicateInfo pred) =>
        pred.CompareOp is not (ScanCompareOp.AlwaysTrue or ScanCompareOp.AlwaysFalse);

    private static bool ConsumesScalarParam(in ScanPredicateInfo pred) =>
        pred.SubPredicates == null &&
        pred.CompareOp is not (ScanCompareOp.AlwaysTrue or ScanCompareOp.AlwaysFalse
            or ScanCompareOp.In or ScanCompareOp.AllIn or ScanCompareOp.Exists);

    private static void EmitLeafPredicate(ref DualEmit d, in ScanPredicateInfo pred, Label failIl, string failName, int rootIdx, ref int inSetIdx, int paramSlot, LocalBuilder readerRefLocal)
    {
        // Sentinel leaves (collapsed MatchAll / MatchNothing inside a group) carry no field and consume no fieldRootPage slot
        if (pred.CompareOp == ScanCompareOp.AlwaysTrue)
        {
            d.CsLine("// always-true (MatchAll sentinel) — no-op");
            return;
        }
        // skip directly to the fail path, nothing to do here
        if (pred.CompareOp == ScanCompareOp.AlwaysFalse)
        {
            d.Il.Emit(OpCodes.Br, failIl);
            d.CsLine(Jump(failName));
            return;
        }

        // Build-time clause label for the residual C# mirror; no IL effect.
        if (pred.FieldName != null)
            d.CsLine($"// {pred.FieldName} [{pred.CompareOp}{(pred.Negated ? ", negated" : string.Empty)}]");

        // reader.Reset();
        d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.ReaderReset);
        d.CsLine("reader.Reset();");

        if (pred.ValueType is ScanValueType.Slice or ScanValueType.SliceLong && pred.CompareOp is ScanCompareOp.Equals or ScanCompareOp.NotEquals)
        {
            EmitNullableEquality(ref d, in pred, failIl, failName, rootIdx, ref inSetIdx, paramSlot, readerRefLocal);
            return;
        }

        EmitLeafComparison(ref d, in pred, failIl, failName, rootIdx, ref inSetIdx, paramSlot, readerRefLocal);
    }

    private static void EmitNullableEquality(ref DualEmit d, in ScanPredicateInfo pred, Label failIl, string failName, int rootIdx, ref int inSetIdx, int paramSlot, LocalBuilder readerRefLocal)
    {
        bool isNotEqual = pred.CompareOp == ScanCompareOp.NotEquals;
        var concrete = d.DefineLabelPair("concreteTarget");
        var leafDone = d.DefineLabelPair("nullCmpDone");

        // if (StringValues[ParamIndex] != null) goto concrete;  — concrete target uses ordinary term comparison
        d.BranchIfStringTargetNotNull(paramSlot, concrete);

        // Null target: decide on reader.IsNull alone (Current is stale, no term comparison possible).
        EmitFindNext(ref d, readerRefLocal, rootIdx);
        if (isNotEqual)
        {
            // != null passes unless the field carries the explicit null-marker term.
            EmitBranchFalse(ref d, leafDone.Il, leafDone.Name);   // no term → concrete/absent → pass
            d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
            d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
            d.Il.Emit(OpCodes.Brtrue, failIl);
            d.CsLine(JumpIf("reader.IsNull", failName));          // null-marker term → fail
            d.Il.Emit(OpCodes.Br, leafDone.Il);                   // concrete term → pass
            d.CsLine(Jump(leafDone.Name));
        }
        else
        {
            // = null passes only when the field carries the explicit null-marker term.
            EmitBranchFalse(ref d, failIl, failName);             // no term → fail
            d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
            d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
            d.Il.Emit(OpCodes.Brfalse, failIl);
            d.CsLine(JumpIf("reader.IsNull is false", failName)); // concrete term → fail
            d.Il.Emit(OpCodes.Br, leafDone.Il);                   // null-marker term → pass
            d.CsLine(Jump(leafDone.Name));
        }

        d.MarkLabel(concrete);
        EmitLeafComparison(ref d, in pred, failIl, failName, rootIdx, ref inSetIdx, paramSlot, readerRefLocal);
        d.MarkLabel(leafDone);
    }

    private static void EmitLeafComparison(ref DualEmit d, in ScanPredicateInfo pred, Label failIl, string failName, int rootIdx, ref int inSetIdx, int paramSlot, LocalBuilder readerRefLocal)
    {
        switch (pred.CompareOp)
        {
            case ScanCompareOp.In or ScanCompareOp.AllIn:
            {
                bool allIn = pred.CompareOp == ScanCompareOp.AllIn;
                var helper = pred.ValueType switch
                {
                    ScanValueType.Long => allIn ? IlEmitterShared.CheckFieldTermAllInLong : IlEmitterShared.CheckFieldTermInLong,
                    ScanValueType.Double => allIn ? IlEmitterShared.CheckFieldTermAllInDouble : IlEmitterShared.CheckFieldTermInDouble,
                    _ => allIn ? IlEmitterShared.CheckFieldTermAllInSlice : IlEmitterShared.CheckFieldTermInSlice,
                };

                // ref reader, fieldRootPage, values[], includeNull
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.CsStack.Push("ref reader");
                d.LoadFieldRootPage(rootIdx);
                d.LoadInValueArray(inSetIdx, pred.ValueType);
                d.LoadInHasNull(inSetIdx);
                d.CallStatic(helper);
                EmitBranch(ref d, failOnTrue: pred.Negated, failIl, failName);
                inSetIdx++;
                return;
            }
            // StartsWith / EndsWith are full-field scans rather than positioning calls — wrap FindNext internally
            case ScanCompareOp.StartsWith or ScanCompareOp.EndsWith:
            {
                var helper = pred.CompareOp == ScanCompareOp.StartsWith
                    ? IlEmitterShared.CheckFieldTermStartsWith
                    : IlEmitterShared.CheckFieldTermEndsWith;

                // ref reader, fieldRootPage, paramSpan
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.CsStack.Push("ref reader");
                d.LoadFieldRootPage(rootIdx);
                d.LoadSliceSpan(paramSlot);
                d.CallStatic(helper);
                // Fail if helper returned false.
                EmitBranchFalse(ref d, failIl, failName);
                return;
            }
            case ScanCompareOp.Exists:
                // reader.FindNext(rootPage); predicate succeeds iff a term exists.
                EmitFindNext(ref d, readerRefLocal, rootIdx);
                EmitBranchFalse(ref d, failIl, failName);
                return;
            case ScanCompareOp.NotEquals when pred.IsSingleValued:
            {
                //   if (!reader.FindNext(rootPage)) goto pass;   // no term → pass
                //   if (reader.IsNull) goto pass;                // null term → pass
                //   if (reader.IsNonExisting) goto pass;         // absent (stale Current) → pass
                //   if (term == target) goto fail;               // else fall through → pass
                var singlePass = d.DefineLabelPair("notEqualPass");

                EmitFindNext(ref d, readerRefLocal, rootIdx);
                EmitBranchFalse(ref d, singlePass.Il, singlePass.Name);         // no term → pass

                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
                d.Il.Emit(OpCodes.Brtrue, singlePass.Il);
                d.CsLine(JumpIf("reader.IsNull", singlePass.Name));       // null term → pass
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNonExisting);
                d.Il.Emit(OpCodes.Brtrue, singlePass.Il);
                d.CsLine(JumpIf("reader.IsNonExisting", singlePass.Name)); // non-existing term (stale Current) → pass

                EmitTypedComparison(ref d, in pred, paramSlot, readerRefLocal);
                EmitBranchTrue(ref d, failIl, failName);                        // term equals → fail

                d.Il.MarkLabel(singlePass.Il);
                d.CsLine($"{singlePass.Name}:");
                return;
            }
            //   while (reader.FindNext(rootPage)) { if (reader.IsNull) continue; if (term == target) goto fail; }
            case ScanCompareOp.NotEquals:
            {
                var loopHead = d.DefineLabelPair("notEqualNext");
                var pass = d.DefineLabelPair("notEqualPass");

                // while (reader.FindNext(rootPage)) — FindNext is the loop condition; a false result
                // exits to pass (no term equals → entry passes this leaf).
                d.Il.MarkLabel(loopHead.Il);
                EmitFindNext(ref d, readerRefLocal, rootIdx);
                d.Il.Emit(OpCodes.Brfalse, pass.Il);
                d.CsLine($"while ({d.CsStack.Pop()})");
                d.CsLine("{");

                // if (reader.IsNull) continue;
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
                d.Il.Emit(OpCodes.Brtrue, loopHead.Il);
                d.CsLine("if (reader.IsNull) continue;");
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNonExisting);
                d.Il.Emit(OpCodes.Brtrue, loopHead.Il);
                d.CsLine("if (reader.IsNonExisting) continue;"); // stale Current for non-existing markers

                // if (term == target) goto fail;  — this reject sits INSIDE the term-scan while, cannot just continue
                EmitTypedComparison(ref d, in pred, paramSlot, readerRefLocal);
                EmitBranchTrue(ref d, failIl, failName == ContinueTarget ? RejectedLabel : failName);      // term equals → fail

                d.Il.Emit(OpCodes.Br, loopHead.Il);           // not equal → next term
                d.CsLine("}");

                d.Il.MarkLabel(pass.Il);                            // loop exited → no term equalled → pass
                return;
            }
            default:
            {
                if (pred.IsSingleValued)
                {
                    // Single-valued field: at most one term per entry, single read + single compare
                    //   if (!reader.FindNext(rootPage)) goto fail; if (reader.IsNull) goto fail;
                    //   if (!<cmp>) goto fail;   // else fall through → pass
                    LabelPair singleNullPass = default;
                    if (pred.IncludeNull)
                        singleNullPass = d.DefineLabelPair("singleNullPass");

                    EmitFindNext(ref d, readerRefLocal, rootIdx);
                    EmitBranchFalse(ref d, failIl, failName);     // no term → fail

                    d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                    d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
                    if (pred.IncludeNull)
                    {
                        // IncludeNull (BETWEEN low AND *): a null-valued doc satisfies the clause — null term → pass
                        d.Il.Emit(OpCodes.Brtrue, singleNullPass.Il);
                        d.CsLine(JumpIf("reader.IsNull", singleNullPass.Name));
                    }
                    else
                    {
                        d.Il.Emit(OpCodes.Brtrue, failIl);
                        d.CsLine(JumpIf("reader.IsNull", failName));  // null term → fail
                    }
                    d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                    d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNonExisting);
                    d.Il.Emit(OpCodes.Brtrue, failIl);
                    d.CsLine(JumpIf("reader.IsNonExisting", failName));  // non-existing term (stale Current) → fail

                    EmitTypedComparison(ref d, in pred, paramSlot, readerRefLocal);
                    EmitBranchFalse(ref d, failIl, failName);     // comparison false → fail
                    // fall through → pass
                    if (pred.IncludeNull)
                    {
                        d.Il.MarkLabel(singleNullPass.Il);
                        d.CsLine($"{singleNullPass.Name}:;");
                    }
                    return;
                }

                var loopHead = d.DefineLabelPair("matchNext");
                var pass = d.DefineLabelPair("matchPass");

                // while (reader.FindNext(rootPage)) — FindNext is the loop condition; a false result
                // exits to fail (no term matched → entry fails this leaf).
                d.Il.MarkLabel(loopHead.Il);
                EmitFindNext(ref d, readerRefLocal, rootIdx);
                d.Il.Emit(OpCodes.Brfalse, failIl);
                d.CsLine($"while ({d.CsStack.Pop()})");
                d.CsLine("{");

                // if (reader.IsNull) ...
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNull);
                if (pred.IncludeNull)
                {
                    // IncludeNull (BETWEEN low AND *): a null-valued doc satisfies the clause — null term → pass
                    d.Il.Emit(OpCodes.Brtrue, pass.Il);
                    d.CsLine(JumpIf("reader.IsNull", pass.Name));
                }
                else
                {
                    d.Il.Emit(OpCodes.Brtrue, loopHead.Il);
                    d.CsLine("if (reader.IsNull) continue;");
                }
                d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
                d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderIsNonExisting);
                d.Il.Emit(OpCodes.Brtrue, loopHead.Il);
                d.CsLine("if (reader.IsNonExisting) continue;"); // stale Current for non-existing markers

                // if (<cmp>) goto pass;
                EmitTypedComparison(ref d, in pred, paramSlot, readerRefLocal);
                EmitBranchTrue(ref d, pass.Il, pass.Name);    // term satisfies → pass

                d.Il.Emit(OpCodes.Br, loopHead.Il);           // not satisfied → next term
                d.CsLine("}");

                d.CsLine(Jump(failName));                     // loop exhausted → none matched → fail
                d.Il.MarkLabel(pass.Il);
                d.CsLine($"{pass.Name}:");
                return;
            }
        }
    }

    /// <summary>Emit <c>reader.FindNext(exec.FieldRootPages[rootIdx])</c>, leaving the bool result
    /// on both the IL evaluation stack and the C# operand stack.</summary>
    private static void EmitFindNext(ref DualEmit d, LocalBuilder readerRefLocal, int rootIdx)
    {
        d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
        d.CsStack.Push("reader");
        d.LoadFieldRootPage(rootIdx);
        d.CallInstance(IlEmitterShared.ReaderFindNext);
    }

    private static void EmitTypedComparison(ref DualEmit d, in ScanPredicateInfo pred, int paramSlot, LocalBuilder readerRefLocal)
    {
        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    EmitLongBetween(ref d, readerRefLocal, paramSlot);
                    break;
                }
                d.LoadReaderCurrentLong(readerRefLocal);
                d.LoadLongParam(paramSlot);
                EmitNumericCompareOp(ref d, pred.CompareOp);
                break;

            case ScanValueType.Double:
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    EmitDoubleBetween(ref d, readerRefLocal, paramSlot);
                    break;
                }
                d.LoadReaderCurrentDouble(readerRefLocal);
                d.LoadDoubleParam(paramSlot);
                EmitNumericCompareOp(ref d, pred.CompareOp, isDouble: true);
                break;

            case ScanValueType.Slice:
            case ScanValueType.SliceLong:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    // Diamond mirroring the long/double Between using SequenceCompareTo(a,b) vs 0.
                    var fail = d.DefineLabelPair("sliceBetweenFail");
                    var done = d.DefineLabelPair("sliceBetweenDone");

                    // a.SequenceCompareTo(low) < 0 → fail
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(paramSlot);
                    d.CallStatic(IlEmitterShared.SequenceCompareTo);
                    d.PushConstInt(0);
                    d.BranchLt(fail);

                    // a.SequenceCompareTo(high) > 0 → fail
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(paramSlot, second: true);
                    d.CallStatic(IlEmitterShared.SequenceCompareTo);
                    d.PushConstInt(0);
                    d.BranchGt(fail);

                    EmitBetweenTail(ref d, fail, done);
                    break;
                }
                if (pred.CompareOp is ScanCompareOp.Equals or ScanCompareOp.NotEquals)
                {
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(paramSlot);
                    d.CallStatic(IlEmitterShared.SequenceEqual);
                    break;
                }
                // Relational: compare SequenceCompareTo result against 0 using the same op.
                d.LoadReaderDecodedSlice(readerRefLocal);
                d.LoadSliceSpan(paramSlot);
                d.CallStatic(IlEmitterShared.SequenceCompareTo);
                d.PushConstInt(0);
                EmitNumericCompareOp(ref d, pred.CompareOp);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(pred.ValueType),  pred.ValueType, "Unexpected predicate value");
        }
    }

    /// <summary>
    /// <paramref name="isDouble"/> selects unordered comparison opcodes for GreaterThanOrEqual/LessThanOrEqual
    /// so a NaN operand correctly fails the comparison. Ordered Clt/Cgt (used when false, and always for
    /// GreaterThan/LessThan/Equals/NotEquals) return false for NaN under ECMA-335 semantics; negating that
    /// via LogicalNot would otherwise make NaN wrongly satisfy ">="/"<=" — the unordered Clt_Un/Cgt_Un
    /// (true on NaN) avoid that when negated. Longs and the Slice/SliceLong SequenceCompareTo-vs-0 path
    /// have no NaN concept, so they keep the ordered opcodes (isDouble: false).
    /// </summary>
    private static void EmitNumericCompareOp(ref DualEmit d, ScanCompareOp op, bool isDouble = false)
    {
        switch (op)
        {
            case ScanCompareOp.Equals:
            case ScanCompareOp.NotEquals:
                // Caller decides whether to invert (NotEqual branches on TRUE-equal → fail).
                d.Ceq();
                break;
            case ScanCompareOp.GreaterThan:
                d.Cgt();
                break;
            case ScanCompareOp.GreaterThanOrEqual:
                // !(a < b)
                if (isDouble)
                    d.CltUn();
                else
                    d.Clt();
                d.LogicalNot();
                break;
            case ScanCompareOp.LessThan:
                d.Clt();
                break;
            case ScanCompareOp.LessThanOrEqual:
                // !(a > b)
                if (isDouble)
                    d.CgtUn();
                else
                    d.Cgt();
                d.LogicalNot();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(op),  op, "Unexpected predicate value");
        }
    }

    private static void EmitLongBetween(ref DualEmit d, LocalBuilder readerRefLocal, int paramSlot)
    {
        var fail = d.DefineLabelPair("betweenFail");
        var done = d.DefineLabelPair("betweenDone");

        d.LoadReaderCurrentLong(readerRefLocal);
        d.LoadLongParam(paramSlot);
        d.BranchLt(fail);

        d.LoadReaderCurrentLong(readerRefLocal);
        d.LoadLongParam(paramSlot, second: true);
        d.BranchGt(fail);

        EmitBetweenTail(ref d, fail, done);
    }

    private static void EmitDoubleBetween(ref DualEmit d, LocalBuilder readerRefLocal, int paramSlot)
    {
        var fail = d.DefineLabelPair("betweenFail");
        var done = d.DefineLabelPair("betweenDone");

        d.LoadReaderCurrentDouble(readerRefLocal);
        d.LoadDoubleParam(paramSlot);
        d.BranchLtDouble(fail);

        d.LoadReaderCurrentDouble(readerRefLocal);
        d.LoadDoubleParam(paramSlot, second: true);
        d.BranchGtUnsigned(fail);

        EmitBetweenTail(ref d, fail, done);
    }

    private static void EmitBetweenTail(ref DualEmit d, LabelPair fail, LabelPair done)
    {
        var tmp = d.DeclareTempBool("between");
        d.Il.Emit(OpCodes.Ldc_I4_1);
        d.CsLine($"{tmp} = true;");
        d.GotoAlways(done);

        d.MarkLabel(fail);
        d.Il.Emit(OpCodes.Ldc_I4_0);
        d.CsLine($"{tmp} = false;");

        d.MarkLabel(done);
        d.PushTempName(tmp);
    }

    private static void EmitBranchFalse(ref DualEmit d, Label ilLabel, string csName) => EmitBranch(ref d, failOnTrue: false, ilLabel, csName);

    private static void EmitBranchTrue(ref DualEmit d, Label ilLabel, string csName) => EmitBranch(ref d, failOnTrue: true, ilLabel, csName);

    private static void EmitBranch(ref DualEmit d, bool failOnTrue, Label ilLabel, string csName)
    {
        d.Il.Emit(failOnTrue ? OpCodes.Brtrue : OpCodes.Brfalse, ilLabel);
        var a = d.CsStack.Pop();
        d.CsLine(JumpIf(failOnTrue ? a : $"!{a}", csName));
    }

    // we use the "continue" target to emit a continue in C# code when jumping to the top of the loop body 
    private const string ContinueTarget = "continue";

    private const string RejectedLabel = "rejected";

    private static string Jump(string csName) => csName == ContinueTarget ? "continue;" : $"goto {csName};";

    private static string JumpIf(string cond, string csName) =>
        csName == ContinueTarget ? $"if ({cond}) continue;" : $"if ({cond}) goto {csName};";

    // if this is true, we need to add a "rejected:" label
    private static bool AnyNotEqual(ReadOnlySpan<ScanPredicateInfo> predicates)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var predicate in predicates )
        {
            ref readonly ScanPredicateInfo p = ref predicate;
            if (p.SubPredicates != null && AnyNotEqual(p.SubPredicates))
                return true;
            if (p.CompareOp == ScanCompareOp.NotEquals)
                return true;
        }

        return false;
    }
}
