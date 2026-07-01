using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Text;
using Voron;

namespace Corax.Querying.Planning;

internal readonly record struct LabelPair(Label Il, string Name);

/// <summary>
/// Dual-backend emission helper. Every primitive emits one IL operation AND
/// the matching effect on a textual C# operand stack — value producers push a
/// textual fragment; control-flow primitives pop fragments and write a C#
/// statement to the <see cref="cs"/> buffer.
///
/// The C# operand stack is parallel to the IL evaluation stack:
/// every IL "push" corresponds to a textual fragment push, every IL "pop"
/// to a textual fragment pop.
/// </summary>
internal ref partial struct DualEmit(ILGenerator il, StringBuilder cs)
{
    public readonly ILGenerator Il = il;
    public readonly Stack<string> CsStack = new();
    private readonly Dictionary<LocalBuilder, string> _locals = new();
    private readonly List<string> _args = [];
    private int _labelCounter = 0;
    private int _tempCounter = 0;

    private byte _contextArgIndex;

    /// <summary>Clause label staged by <see cref="SetPendingComment"/> and emitted as a TRAILING comment
    /// on the next <see cref="CsCall"/> line (the op's primary statement), so the label sits on the call
    /// it describes rather than floating on its own line above the cancellation check.</summary>
    private string _pendingComment;

    public void CsLine(string line) => cs.AppendLine(line);

    /// <summary>Stage a label to trail the next <see cref="CsCall"/>. Only ops whose primary statement is
    /// emitted via <see cref="CsCall"/> carry a label, so the staged comment is always consumed before the
    /// next is set — assert that here to catch a future op kind that labels itself without a CsCall.</summary>
    public void SetPendingComment(string comment)
    {
        Debug.Assert(_pendingComment == null,
            $"DualEmit: clause label '{_pendingComment}' was staged but never emitted before staging '{comment}'");
        _pendingComment = comment;
    }

    /// <summary>Emit an op's primary call statement, attaching any label staged via
    /// <see cref="SetPendingComment"/> as a trailing C# comment.</summary>
    public void CsCall(string line)
    {
        if (_pendingComment == null)
        {
            cs.AppendLine(line);
            return;
        }

        cs.Append(line).Append("    // ").AppendLine(_pendingComment);
        _pendingComment = null;
    }

    public LabelPair DefineLabelPair(string prefix) => new(Il.DefineLabel(), $"{prefix}_{_labelCounter++}");

    /// <summary>Define a label with an exact name (no counter suffix).
    /// Use for well-known labels like "Done" or "EntryScan".</summary>
    public LabelPair DefineNamedLabel(string exactName) => new(Il.DefineLabel(), exactName);

    public void MarkLabel(LabelPair l)
    {
        Il.MarkLabel(l.Il);
        cs.Append(l.Name);
        cs.AppendLine(":");
        Debug.Assert(CsStack.Count == 0,
            $"DualEmit: C# operand stack not empty at label {l.Name}: [{string.Join(", ", CsStack)}]");
    }

    private string NewTempName(string hint) => $"{hint}_{_tempCounter++}";

    public string DeclareTempBool(string hint)
    {
        var name = NewTempName(hint);
        CsLine($"bool {name};");
        return name;
    }

    public void PushTempName(string name) => CsStack.Push(name);

    public LocalBuilder DeclareLocal(Type type, string csName)
    {
        var local = Il.DeclareLocal(type);
        _locals[local] = csName;
        return local;
    }

    public LocalBuilder DeclareLocalRef(Type type, string csName)
    {
        var local = Il.DeclareLocal(type.MakeByRefType());
        _locals[local] = csName;
        return local;
    }

    public string GetLocalName(LocalBuilder local) => _locals[local];

    public void LoadLocal(LocalBuilder local)
    {
        Il.Emit(OpCodes.Ldloc, local);
        CsStack.Push(_locals[local]);
    }

    public void StoreLocalConst(LocalBuilder local, int value)
    {
        IlEmitterShared.EmitLdcI4(Il, value);
        Il.Emit(OpCodes.Stloc, local);
        CsLine($"{_locals[local]} = {value};");
    }

    public void IncrementLocal(LocalBuilder local)
    {
        Il.Emit(OpCodes.Ldloc, local);
        Il.Emit(OpCodes.Ldc_I4_1);
        Il.Emit(OpCodes.Add);
        Il.Emit(OpCodes.Stloc, local);
        CsLine($"{_locals[local]}++;");
    }

    public byte RegisterArg(string csName)
    {
        var argIdx = _args.Count;
        _args.Add(csName);
        return checked((byte)argIdx);
    }

    public string GetArgName(byte index) => _args[index];

    public void SetContextArg(byte index) => _contextArgIndex = index;

    private string ContextArgName => _args[_contextArgIndex];

    private void LoadContextArg() => Il.Emit(OpCodes.Ldarg_S, _contextArgIndex);

    public void LoadArgAddress(byte index)
    {
        Il.Emit(OpCodes.Ldarga_S, index);
        CsStack.Push(_args[index]);
    }

    public void EmitRetVoid()
    {
        Il.Emit(OpCodes.Ret);
        CsLine("return;");
    }

    public void EmitReturn()
    {
        Il.Emit(OpCodes.Ret);
        var val = CsStack.Pop();
        CsLine($"return {val};");
    }

    public void PushConstBool(bool v)
    {
        Il.Emit(v ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        CsStack.Push(v ? "true" : "false");
    }

    public void PushConstInt(int v)
    {
        IlEmitterShared.EmitLdcI4(Il, v);
        CsStack.Push(v.ToString());
    }

    public void LoadReaderCurrentLong(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentLong);
        CsStack.Push("reader.CurrentLong");
    }

    public void LoadReaderCurrentDouble(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentDouble);
        CsStack.Push("reader.CurrentDouble");
    }

    public void LoadReaderDecodedSlice(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrent);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CompactKeyDecoded);
        CsStack.Push("reader.Current.Decoded()");
    }

    private string EmitResidualParamIndex(int slot, bool second)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, second ? IlEmitterShared.ResidualParamSlot2 : IlEmitterShared.ResidualParamSlot1);
        IlEmitterShared.EmitLdcI4(Il, slot);
        Il.Emit(OpCodes.Ldelem_I4);

        return $"{ContextArgName}.{(second ? "ResidualParamSlot2" : "ResidualParamSlot1")}[{slot}]";
    }

    public void LoadLongParam(int slot, bool second = false)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ResidualLongs);
        var expr = EmitResidualParamIndex(slot, second);
        Il.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"{ContextArgName}.LongValues[{expr}]");
    }

    public void LoadDoubleParam(int slot, bool second = false)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ResidualDoubles);
        var expr = EmitResidualParamIndex(slot, second);
        Il.Emit(OpCodes.Ldelem_R8);
        CsStack.Push($"{ContextArgName}.DoubleValues[{expr}]");
    }

    public void LoadSliceSpan(int slot, bool second = false)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.AnalyzedSlices);
        var expr = EmitResidualParamIndex(slot, second);
        Il.Emit(OpCodes.Ldelema, typeof(Slice));
        Il.Emit(OpCodes.Call, IlEmitterShared.SliceAsReadOnlySpan);
        CsStack.Push($"{ContextArgName}.AnalyzedSlices[{expr}].AsReadOnlySpan()");
    }

    public void LoadFieldRootPage(int rootIdx)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ResidualFieldRootPages);
        IlEmitterShared.EmitLdcI4(Il, rootIdx);
        Il.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"{ContextArgName}.FieldRootPages[{rootIdx}]");
    }

    public void LoadInValueArray(int idx, ScanValueType valueType)
    {
        var (arrayField, spanCtor, csArray, csElem) = valueType switch
        {
            ScanValueType.Long => (IlEmitterShared.ResidualLongs, IlEmitterShared.ReadOnlySpanLongCtor, $"{ContextArgName}.LongValues", "long"),
            ScanValueType.Double => (IlEmitterShared.ResidualDoubles, IlEmitterShared.ReadOnlySpanDoubleCtor, $"{ContextArgName}.DoubleValues", "double"),
            _ => (IlEmitterShared.AnalyzedSlices, IlEmitterShared.ReadOnlySpanSliceCtor, $"{ContextArgName}.AnalyzedSlices", "Slice"),
        };
        // flat array
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, arrayField);
        // start = exec.ResidualInSets[idx].Base, length = exec.ResidualInSets[idx].Count
        EmitLoadInSetField(idx, IlEmitterShared.ResidualInValuesBase);
        EmitLoadInSetField(idx, IlEmitterShared.ResidualInValuesCount);
        Il.Emit(OpCodes.Newobj, spanCtor);
        CsStack.Push($"new ReadOnlySpan<{csElem}>({csArray}, {ContextArgName}.ResidualInSets[{idx}].Base, {ContextArgName}.ResidualInSets[{idx}].Count)");
    }

    public void LoadInHasNull(int idx)
    {
        EmitLoadInSetField(idx, IlEmitterShared.ResidualInValuesHasNull);
        CsStack.Push($"{ContextArgName}.ResidualInSets[{idx}].HasNull");
    }

    // if (exec.StringValues[exec.ResidualParamSlot1[slot]] != null) goto label;
    public void BranchIfStringTargetNotNull(int slot, LabelPair l)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ResidualStringValues);
        var expr = EmitResidualParamIndex(slot, second: false);
        Il.Emit(OpCodes.Ldelem_Ref);
        Il.Emit(OpCodes.Brtrue, l.Il);
        CsLine($"if ({ContextArgName}.StringValues[{expr}] != null) goto {l.Name};");
    }

    private void EmitLoadInSetField(int idx, FieldInfo field)
    {
        LoadContextArg();
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ResidualInSets);
        IlEmitterShared.EmitLdcI4(Il, idx);
        Il.Emit(OpCodes.Ldelema, typeof(ResidualInValues));
        Il.Emit(OpCodes.Ldfld, field);
    }

    public void Ceq()
    {
        Il.Emit(OpCodes.Ceq);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsStack.Push($"({a} == {b})");
    }

    public void Clt()
    {
        Il.Emit(OpCodes.Clt);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsStack.Push($"({a} < {b})");
    }

    public void Cgt()
    {
        Il.Emit(OpCodes.Cgt);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsStack.Push($"({a} > {b})");
    }

    public void LogicalNot()
    {
        Il.Emit(OpCodes.Ldc_I4_0);
        Il.Emit(OpCodes.Ceq);
        var a = CsStack.Pop();
        CsStack.Push($"{a} is false");
    }

    /// <summary>Emits a static call and mirrors it as <c>Type.Method(arg0, arg1, ...)</c>.
    /// Operands popped = parameter count.</summary>
    public void CallStatic(MethodInfo method)
    {
        Il.Emit(OpCodes.Call, method);
        var args = PopArgs(method.GetParameters().Length);
        CsStack.Push($"{method.DeclaringType!.Name}.{method.Name}({string.Join(", ", args)})");
    }

    /// <summary>Emits an instance call and mirrors it as <c>receiver.Method(arg0, ...)</c>.
    /// Operands popped = parameter count + 1 (the receiver, which an instance method does not list as a parameter).</summary>
    public void CallInstance(MethodInfo method)
    {
        Il.Emit(OpCodes.Call, method);
        var args = PopArgs(method.GetParameters().Length + 1);
        CsStack.Push($"{args[0]}.{method.Name}({string.Join(", ", args[1..])})");
    }

    private Span<string> PopArgs(int arity)
    {
        var args = new string[arity];
        for (int i = arity - 1; i >= 0; i--) args[i] = CsStack.Pop();
        return args;
    }

    public void BranchLt(LabelPair l)
    {
        Il.Emit(OpCodes.Blt, l.Il);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchLtDouble(LabelPair l)
    {
        Il.Emit(OpCodes.Blt_Un, l.Il);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchGt(LabelPair l)
    {
        Il.Emit(OpCodes.Bgt, l.Il);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void BranchGtUnsigned(LabelPair l)
    {
        Il.Emit(OpCodes.Bgt_Un, l.Il);
        var b = CsStack.Pop();
        var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void GotoAlways(LabelPair l)
    {
        Il.Emit(OpCodes.Br, l.Il);
        CsLine($"goto {l.Name};");
    }
}
