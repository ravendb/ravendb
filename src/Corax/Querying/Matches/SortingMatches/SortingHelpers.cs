using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Querying.Matches.SortingMatches;

internal static class SortingHelpers
{
    public const long InvalidTermId = -1;
    public static void ReplaceNullAndNonExistingTermIds(Span<long> buffer, long nonExistingTermId, long nullTermId, long replaceWith)
    {
        if (nonExistingTermId == InvalidTermId && nullTermId == InvalidTermId)
            return;
        
        int idX = 0;
        ref var bufferRef = ref MemoryMarshal.GetReference(buffer);
        if (AdvInstructionSet.IsAcceleratedVector512)
        {
            var N = Vector512<long>.Count;
            var nonExistingVector = Vector512.Create(nonExistingTermId);
            var nullVector = Vector512.Create(nullTermId);
                
            for (; idX + N <= buffer.Length; idX += N)
            {
                var currentMask = Vector512.LoadUnsafe(ref Unsafe.Add(ref bufferRef, idX));
                if (Vector512.EqualsAny(currentMask, nullVector) || Vector512.EqualsAny(currentMask, nonExistingVector))
                {
                    for (int i = 0; i < N; i++)
                    {
                        if (buffer[idX + i] == nonExistingTermId || buffer[idX + i] == nullTermId)
                            buffer[idX + i] = replaceWith;
                    }
                }
            }
                
        }
            
        for (; idX < buffer.Length; idX++)
        {
            if (buffer[idX] == nonExistingTermId || buffer[idX] == nullTermId)
                buffer[idX] = replaceWith;
        }
    }
}
