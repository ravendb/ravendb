using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>Statement-level emission helpers used by <see cref="QueryIlEmitter"/>.</summary>
[StructLayout(LayoutKind.Auto)]
internal ref partial struct DualEmit
{
    public void IlLoadBitmapRef(int slot)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxBitmaps);
        IlEmitterShared.EmitLdcI4(Il, slot);
        Il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
    }

    public void IlCancellationCheck()
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldflda, IlEmitterShared.CtxToken);
        Il.Emit(OpCodes.Call, IlEmitterShared.ThrowIfCancelled);
        CsLine("ctx.Token.ThrowIfCancellationRequested();");
    }

    private void IlAdvanceCursor(LocalBuilder cursorVar)
    {
        Il.Emit(OpCodes.Ldloc, cursorVar);
        Il.Emit(OpCodes.Ldc_I4_1);
        Il.Emit(OpCodes.Add);
        Il.Emit(OpCodes.Stloc, cursorVar);
        CsLine("cursor++;");
    }

    public void EmitBitmapBinaryOp(int target, int source, MethodInfo ilMethod)
    {
        IlLoadBitmapRef(target);
        IlLoadBitmapRef(source);
        Il.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{target}].{ilMethod.Name}(ref ctx.Bitmaps[{source}]);");
    }

    public void EmitBitmapUnaryCall(int slot, MethodInfo ilMethod)
    {
        IlLoadBitmapRef(slot);
        Il.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{slot}].{ilMethod.Name}();");
    }

    public void EmitBitmapEmptyGoto(int slot, LabelPair done)
    {
        IlLoadBitmapRef(slot);
        Il.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
        Il.Emit(OpCodes.Brtrue, done.Il);
        CsLine($"if (ctx.Bitmaps[{slot}].IsEmpty) goto {done.Name};");
    }

    public void EmitLimitReachedGoto(LabelPair done)
    {
        IlLoadBitmapRef(0);
        Il.Emit(OpCodes.Call, IlEmitterShared.ComputeCountMethod);
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxLimit);
        Il.Emit(OpCodes.Bge, done.Il);
        CsLine($"if (ctx.Bitmaps[0].ComputeCount() >= ctx.Limit) goto {done.Name};");
    }

    /// <summary>Arm the per-op truncation budget: ctx.OpLimit = ctx.Limit. Emitted only before the first op
    /// that grows slot 0 monotonically to the result, so every fill/AND upstream of a narrowing op (which read
    /// the default unlimited OpLimit) materialize their full posting list instead of a limit-truncated prefix.</summary>
    public void EmitArmOpLimit()
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxLimit);
        Il.Emit(OpCodes.Conv_I8);
        Il.Emit(OpCodes.Stfld, IlEmitterShared.CtxOpLimit);
        CsLine("ctx.OpLimit = ctx.Limit;");
    }

    public void EmitCancelledCursorSlotCall(LocalBuilder cursorVar, MethodInfo ilMethod, int bitmapSlot, bool advanceCursor = true)
    {
        IlCancellationCheck();
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldloc, cursorVar);
        Il.Emit(OpCodes.Ldc_I4, bitmapSlot);
        Il.Emit(OpCodes.Call, ilMethod);
        CsCall($"QueryPrimitives.{ilMethod.Name}(ctx, cursor, bitmapSlot: {bitmapSlot});");
        // The advance after the last cursor-consuming op is a dead store; the caller suppresses it there.
        if (advanceCursor)
            IlAdvanceCursor(cursorVar);
    }

    public void EmitFillAllEntries(int bitmapSlot)
    {
        IlCancellationCheck();
        Il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(Il, bitmapSlot);
        Il.Emit(OpCodes.Call, IlEmitterShared.CtxFillAllEntries);
        CsLine($"QueryPrimitives.{IlEmitterShared.CtxFillAllEntries.Name}(ctx, {bitmapSlot});");
    }

    public void EmitGotoDone(LabelPair done)
    {
        Il.Emit(OpCodes.Br, done.Il);
        CsLine($"goto {done.Name};");
    }
}
